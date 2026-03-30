import os
import re
import math
import numpy as np
import pandas as pd

# ---------------- PSS/E Imports ----------------
import psse36
import psspy
import pssarrays
import redirect

psspy.psseinit(50000)

# =========================
# USER INPUTS
# =========================
#CASE_PATH   = r"4_Bus_Test_System.sav"
#INPUT_XLSX  = r"input_4_bus_test_system.xlsx"
#OUTPUT_XLSX = r"SCR_SCRIF_results_4_bus_test_system.xlsx"

CASE_PATH   = r"23_Bus_Test_System.sav"
INPUT_XLSX  = r"input_23_bus_test_system.xlsx"
OUTPUT_XLSX = r"SCR_SCRIF_results_23_bus_test_system.xlsx"

#CASE_PATH   = r"ALL India M-1 Basecase Mar'26 Solar-peak.sav"
#INPUT_XLSX  = r"input_NR.xlsx"
#OUTPUT_XLSX = r"SCR_SCRIF_results_All_India.xlsx"

# Power flow solve options
FNSL_OPTS = [0, 0, 0, 1, 0, 0, 0, 0]

# ΔQ settings for IF (default; can be overridden per bus in Excel)
DEFAULT_Q_STEP_MVAR = 10.0
# IF perturbation mode:
# - "adaptive": start with per-bus Q_step_MVAr and automatically increase ΔQ until |ΔVj| >= DVJ_MIN (current behavior)
# - "fixed_skip": use only the per-bus Q_step_MVAr once; if |ΔVj| < DVJ_MIN, skip the IF column (no adaptive growth)
IF_Q_MODE = "fixed_skip"



# Minimum |ΔVj| threshold for IF normalization (avoid dividing by tiny number)
DVJ_MIN = 1e-6

# Perturbation handling
ADAPTIVE_Q_MAX_MVAR = 500.0       # cap to avoid insane injection
ADAPTIVE_Q_GROWTH = 2.0            # growth factor/multiplier each retry
SKIP_WEAK_COLUMNS = True           # if still too small at max, skip that IF column
BASE_STATE_DRIFT_TOL = 1e-6        # max allowed |ΔV| between saved scenario and reloaded base state

# Temp snapshots
BASE_TEMP     = os.path.join(os.getcwd(), "base_temp.sav")
SCENARIO_TEMP = os.path.join(os.getcwd(), "scenario_temp.sav")

# Dedicated fixed-shunt ID for perturbation (PSSE needs 2-char)
SCRIF_SHUNT_ID = "Z1"

# Folder for PSSE logs
LOG_DIR = os.path.join(os.getcwd(), "psse_logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ========== Failure detection ==========
FAIL_KEYWORDS = [
    "ITERATION LIMIT",
    "BLOWN UP",
    "BLOW UP",
    "DIVERG",
    "NOT CONVERG",
    "NETWORK NOT SOLVED",
    "SOLUTION FAILED",
    "SYSTEM HAS NOT BEEN SOLVED",
    "NO CONVERGENCE",
]
MISMATCH_HARD_FAIL_MVA = 1000.0


# ==========================================================
# --------- SAFE WRAPPERS (PSSE RETURN STYLE PROOF) --------
# ==========================================================
def _unwrap(ret):
    """Handle PSSE functions that may return value OR (ierr, value)."""
    if isinstance(ret, (tuple, list)):
        return int(ret[0]), ret[1]
    return 0, ret

def get_system_mva_base() -> float:
    ierr, sbase = _unwrap(psspy.sysmva())
    if ierr:
        raise RuntimeError(f"sysmva failed ierr={ierr}")
    return float(sbase)

def get_bus_kv(bus: int) -> float:
    ierr, kv = _unwrap(psspy.busdat(bus, "BASE"))
    if ierr:
        raise RuntimeError(f"busdat(BASE) failed bus={bus} ierr={ierr}")
    return float(kv)

def get_bus_pu(bus: int) -> float:
    ierr, vpu = _unwrap(psspy.busdat(bus, "PU"))
    if ierr:
        raise RuntimeError(f"busdat(PU) failed bus={bus} ierr={ierr}")
    return float(vpu)


# ==========================================================
# --------- PSSE OUTPUT LOGGING + FAILURE CLASSIFY ---------
# ==========================================================
class PsseLogCapture:
    """Redirect PSSE report/progress/alert/prompt outputs to a log file for the duration of a 'with' block."""
    def __init__(self, log_path: str):
        self.log_path = log_path

    def __enter__(self):
        try:
            with open(self.log_path, "w") as f:
                f.write("")
        except Exception:
            pass

        try:
            redirect.psse2py()
        except Exception:
            pass

        psspy.report_output(2, self.log_path, [0, 0])
        psspy.progress_output(2, self.log_path, [0, 0])
        psspy.alert_output(2, self.log_path, [0, 0])
        psspy.prompt_output(2, self.log_path, [0, 0])
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        psspy.report_output(6, "", [0, 0])
        psspy.progress_output(6, "", [0, 0])
        psspy.alert_output(6, "", [0, 0])
        psspy.prompt_output(6, "", [0, 0])
        return False


def log_mark(log_path: str | None) -> int:
    """Return current byte size of log file (used as a marker)."""
    if not log_path:
        return 0
    try:
        return os.path.getsize(log_path)
    except Exception:
        return 0

def read_log_upper_from(log_path: str | None, start_pos: int) -> str:
    """Read log text written after start_pos (byte offset)."""
    if not log_path:
        return ""
    try:
        with open(log_path, "r", errors="ignore") as f:
            f.seek(start_pos)
            return f.read().upper()
    except Exception:
        return ""

def log_has_failure_keywords(log_text_upper: str) -> bool:
    return any(k in log_text_upper for k in FAIL_KEYWORDS)

def classify_psse_failure(log_path: str | None, ierr: int | None = None, tail_txt_upper: str | None = None) -> str:
    txt = (tail_txt_upper or "").upper()

    if "ITERATION LIMIT" in txt:
        return "ITERATION LIMIT EXCEEDED"
    if "BLOWN UP" in txt or "BLOW UP" in txt:
        return "CASE BLOWN UP"
    if "DIVERG" in txt:
        return "DIVERGED"
    if "NOT CONVERG" in txt or "NO CONVERGENCE" in txt:
        return "NOT CONVERGED"
    if "NETWORK NOT SOLVED" in txt or "SYSTEM HAS NOT BEEN SOLVED" in txt:
        return "NETWORK NOT SOLVED"
    if "SOLUTION FAILED" in txt:
        return "SOLUTION FAILED"

    if ierr is not None and ierr != 0:
        return f"FNSL FAILED (ierr={ierr})"
    return "CASE FAILED"

def get_system_mismatch_mva() -> float | None:
    try:
        ierr, msm = _unwrap(psspy.sysmsm())
        if ierr == 0:
            return float(msm)
    except Exception:
        pass
    return None


def solve_case_safe(log_path: str | None = None) -> tuple[bool, str]:
    """
    Robust solve:
    - Run FNSL
    - If it fails, try FDNS then FNSL again
    - IMPORTANT: Failure keywords are checked ONLY in the log tail corresponding
      to the LAST solve attempt (prevents false failures if first attempt printed warnings)
    - Optional mismatch hard fail
    """
    start = log_mark(log_path)
    ierr, _ = _unwrap(psspy.fnsl(FNSL_OPTS))

    if ierr != 0:
        try:
            psspy.fdns([0, 0, 0, 1, 1, 0, 99, 0])
            start = log_mark(log_path)  # reset tail marker for retry attempt
            ierr, _ = _unwrap(psspy.fnsl(FNSL_OPTS))
        except Exception:
            pass

    if ierr != 0:
        tail = read_log_upper_from(log_path, start)
        return False, classify_psse_failure(log_path, ierr=ierr, tail_txt_upper=tail)

    # Scan only new text produced by the LAST successful solve
    tail = read_log_upper_from(log_path, start)
    if tail and log_has_failure_keywords(tail):
        return False, classify_psse_failure(log_path, ierr=None, tail_txt_upper=tail)

    msm = get_system_mismatch_mva()
    if msm is not None and msm > MISMATCH_HARD_FAIL_MVA:
        return False, f"HIGH MISMATCH ({msm:.2f} MVA)"

    return True, "OK"


def solve_case_safe_n(log_path: str | None = None, n: int = 3) -> tuple[bool, str]:
    last = "OK"
    for _ in range(max(1, int(n))):
        ok, msg = solve_case_safe(log_path)
        last = msg
        if not ok:
            return False, msg
    return True, last


# ==========================================================
# INPUT READING
# ==========================================================
def read_inputs(xlsx_path: str):
    df_bus = pd.read_excel(xlsx_path, sheet_name="Bus")
    df_con = pd.read_excel(xlsx_path, sheet_name="Contingency")

    df_bus.columns = df_bus.columns.str.strip()
    df_con.columns = df_con.columns.str.strip()

    need_bus = {"BusNumber", "Installed Capacity"}
    need_con = {"Sl No", "Type", "FromNumber", "ToNumber", "ID"}
    if not need_bus.issubset(df_bus.columns):
        raise ValueError(f"Bus sheet missing columns: {sorted(list(need_bus - set(df_bus.columns)))}")
    if not need_con.issubset(df_con.columns):
        raise ValueError(f"Contingency sheet missing columns: {sorted(list(need_con - set(df_con.columns)))}")

    df_bus["BusNumber"] = df_bus["BusNumber"].astype(int)
    df_bus["Installed Capacity"] = df_bus["Installed Capacity"].astype(float)

    if "Q_step_MVAr" not in df_bus.columns:
        print(
            f"WARNING: 'Q_step_MVAr' column not found in Bus sheet. "
            f"Using default ΔQ = {DEFAULT_Q_STEP_MVAR} MVAr for all buses."
        )
        df_bus["Q_step_MVAr"] = DEFAULT_Q_STEP_MVAR
    else:
        df_bus["Q_step_MVAr"] = pd.to_numeric(df_bus["Q_step_MVAr"], errors="coerce")

        missing_mask = df_bus["Q_step_MVAr"].isna()
        if missing_mask.any():
            print(
                f"WARNING: 'Q_step_MVAr' blank/invalid for {missing_mask.sum()} bus(es). "
                "These buses will be skipped."
            )

        # Treat blank/invalid as zero so they are skipped later by existing logic
        df_bus["Q_step_MVAr"] = df_bus["Q_step_MVAr"].fillna(0.0)

    # Optional MVA deduction sheet. If missing/incomplete, fall back safely to zero deduction.
    df_ded = pd.DataFrame(columns=["BusNumber", "MVA Capacity", "Percentage Deduction"])
    try:
        df_ded = pd.read_excel(xlsx_path, sheet_name="MVA_Deduction")
        df_ded.columns = df_ded.columns.str.strip()
    except Exception:
        df_ded = pd.DataFrame(columns=["BusNumber", "MVA Capacity", "Percentage Deduction"])

    # If contingency sheet is empty (no rows), skip type conversion
    if not df_con.dropna(how="all").empty:

        df_con["Sl No"] = df_con["Sl No"].astype(int)
        df_con["FromNumber"] = df_con["FromNumber"].astype(int)
        df_con["ToNumber"] = df_con["ToNumber"].astype(int)
        df_con["ID"] = df_con["ID"].astype(str).str.strip()
        df_con["Type"] = df_con["Type"].astype(str).str.strip()

        if "Contingency" in df_con.columns:
            df_con["Contingency"] = df_con["Contingency"].astype(str).str.strip()
    else:
        # keep empty dataframe so code runs base case only
        df_con = df_con.iloc[0:0]

    return df_bus, df_con, df_ded


# ==========================================================
# SUBSYSTEM + VOLTAGES
# ==========================================================
def set_bus_subsystem(sid: int, buses: list[int]):
    psspy.bsys(sid, 0, [0.0, 0.12e4], 0, [], len(buses), buses, 0, [], 0, [])

def get_bus_vpu(buses: list[int]) -> dict[int, float]:
    set_bus_subsystem(1, buses)
    ierr1, nums = psspy.abusint(1, 1, ["NUMBER"])
    ierr2, vpu  = psspy.abusreal(1, 1, ["PU"])
    if ierr1 or ierr2:
        raise RuntimeError(f"abusint/abusreal error ierr={ierr1},{ierr2}")
    return dict(zip(nums[0], vpu[0]))


# ==========================================================
# IEC 60909 SETTINGS
# ==========================================================
def apply_iec_gui_settings_for_selected_buses():
    psspy.short_circuit_units(1)
    psspy.short_circuit_z_units(1)
    psspy.short_circuit_coordinates(1)
    psspy.short_circuit_z_coordinates(1)

    intgar = [1,0,0,0,1,0,1,0,0,0,1,1,0,0,0,0,2]
    realar = [0.1, 1.1]
    psspy.iecs_4(1, 0, intgar, realar, "", "", "")


# ==========================================================
# IEC 60909 SCMVA
# ==========================================================
def _iter_fault_bus_data(rlst):
    """
    Preserve the original working behavior first by pairing rlst.fltbus with
    rlst.flt3ph.values(), but add light hardening for cases where the object
    shape differs or counts do not align.
    """
    fltbus = list(getattr(rlst, "fltbus", []))
    flt3ph = getattr(rlst, "flt3ph", None)

    # Original behavior: pair buses with dict values in the returned order.
    if hasattr(flt3ph, "values"):
        values = list(flt3ph.values())
        if len(values) == len(fltbus):
            for bus_number, fault_data in zip(fltbus, values):
                yield bus_number, fault_data
            return

    # Fallback: keyed lookup if available.
    if hasattr(flt3ph, "get"):
        for bus_number in fltbus:
            fault_data = flt3ph.get(bus_number, None)
            if fault_data is None:
                fault_data = flt3ph.get(str(bus_number), None)
            yield bus_number, fault_data
        return

    # Last resort: if flt3ph itself is iterable in bus order.
    try:
        for bus_number, fault_data in zip(fltbus, flt3ph):
            yield bus_number, fault_data
    except Exception:
        for bus_number in fltbus:
            yield bus_number, None

def _extract_scmva_from_fault_dict(fd: dict):
    if not isinstance(fd, dict):
        return None
    candidates = ["scmva", "mva", "sc_mva", "faultmva", "flt_mva", "scmva3ph"]
    lower_map = {str(k).lower(): k for k in fd.keys()}
    for c in candidates:
        if c in lower_map:
            try:
                return float(fd[lower_map[c]])
            except Exception:
                pass
    return None

def get_fault_mva_iec60909(buses: list[int]) -> dict[int, float]:
    set_bus_subsystem(1, buses)
    apply_iec_gui_settings_for_selected_buses()

    rlst = pssarrays.iecs_currents(sid=1, all=0, flt3ph=1, cfactor=3, ucfactor=1.0)

    out = {}
    for bus_number, fault_data in _iter_fault_bus_data(rlst):
        b = int(bus_number)

        direct = _extract_scmva_from_fault_dict(fault_data)
        if direct is not None and np.isfinite(direct) and direct > 0.0:
            out[b] = direct
            continue

        ia1 = fault_data.get("ia1", None) if isinstance(fault_data, dict) else None
        if ia1 is None:
            out[b] = np.nan
            continue

        I_ka = abs(ia1) / 1000.0
        V_kv = get_bus_kv(b)
        out[b] = math.sqrt(3.0) * V_kv * I_ka

    return out


def build_fault_mva_deduction_map(df_ded: pd.DataFrame, valid_buses: list[int]) -> dict[int, float]:
    """
    Optional input sheet reader for per-bus fault MVA deduction.
    Safe behavior:
    - missing sheet / missing required columns -> all deductions = 0
    - blank/invalid cells -> that row contributes 0 deduction
    - buses not present in Bus sheet -> ignored
    Percentage Deduction accepts either:
    - percent style values (100 = 100%, 50 = 50%)
    - fraction style values (1.0 = 100%, 0.5 = 50%)
    """
    out = {int(b): 0.0 for b in valid_buses}

    if df_ded is None or df_ded.empty:
        return out

    need = {"BusNumber", "MVA Capacity", "Percentage Deduction"}
    cols = {str(c).strip() for c in df_ded.columns}
    if not need.issubset(cols):
        missing = sorted(list(need - cols))
        print(
            f"WARNING: Optional 'MVA_Deduction' sheet missing columns: {missing}. "
            "Ignoring fault MVA deductions."
        )
        return out

    work = df_ded.copy()
    work["BusNumber"] = pd.to_numeric(work["BusNumber"], errors="coerce")
    work["MVA Capacity"] = pd.to_numeric(work["MVA Capacity"], errors="coerce").fillna(0.0)
    work["Percentage Deduction"] = pd.to_numeric(work["Percentage Deduction"], errors="coerce").fillna(0.0)
    work = work.dropna(subset=["BusNumber"])

    if work.empty:
        return out

    work["BusNumber"] = work["BusNumber"].astype(int)
    work = work[work["BusNumber"].isin(valid_buses)]

    for _, row in work.iterrows():
        b = int(row["BusNumber"])
        mva_cap = float(row["MVA Capacity"])
        pct = float(row["Percentage Deduction"])

        # Support both 100-based percent entry and fractional entry.
        pct_fraction = (pct / 100.0) if abs(pct) > 1.0 else pct
        out[b] = mva_cap * pct_fraction

    return out


def apply_fault_mva_deduction(
    scmva: dict[int, float],
    deduction_mva_by_bus: dict[int, float],
    buses: list[int]
) -> dict[int, float]:
    out = {}
    for b in buses:
        raw = scmva.get(b, np.nan)
        ded = float(deduction_mva_by_bus.get(b, 0.0))
        out[b] = (raw - ded) if np.isfinite(raw) else np.nan
    return out


# ==========================================================
# FIXED SHUNT PERTURBATION (PSSE 36.5: shunt_data_2 / shunt_chng_2)
# ==========================================================
def set_fixed_shunt_B_mvar_at_1pu(bus: int, sh_id: str, b_mvar_at_1pu: float):
    ierr = 1
    try:
        ierr = psspy.shunt_chng_2(bus, sh_id, [1], [0.0, float(b_mvar_at_1pu)])
    except Exception:
        ierr = 1

    if ierr != 0:
        ierr = psspy.shunt_data_2(bus, sh_id, [1], [0.0, float(b_mvar_at_1pu)])

    if ierr != 0:
        raise RuntimeError(f"shunt_data_2/shunt_chng_2 failed bus={bus} id={sh_id} ierr={ierr}")


def add_fixed_shunt_deltaQ(bus: int, dq_mvar: float, sh_id: str = SCRIF_SHUNT_ID):
    vpu = max(get_bus_pu(bus), 1e-4)
    b_1pu = float(dq_mvar) / (vpu * vpu)
    set_fixed_shunt_B_mvar_at_1pu(bus, sh_id, b_1pu)


def case_sort_key(x):
    s = str(x).strip()
    if s == "BASE":
        return -1
    m = re.fullmatch(r"Case-(\d+)", s)
    if m:
        return int(m.group(1))
    return 10**9


def is_hard_scrif_failure(reason: str) -> bool:
    s = str(reason).upper()
    hard_terms = [
        "CASE BLOWN UP",
        "ITERATION LIMIT",
        "DIVERGED",
        "NOT CONVERGED",
        "NETWORK NOT SOLVED",
        "SOLUTION FAILED",
        "FAILED:",
        "FNSL FAILED",
        "HIGH MISMATCH",
        "BASE STATE DRIFT",
    ]
    return any(term in s for term in hard_terms)


# ==========================================================
# IF MATRIX (returns ΔQ used table)
# ==========================================================
def compute_if_matrix(
    buses: list[int],
    q_step_by_bus: dict[int, float],
    log_path: str,
    case_name: str,
    q_mode: str = IF_Q_MODE
) -> tuple[pd.DataFrame, dict[int, str], list[dict], str | None]:
    dq_records: list[dict] = []
    col_status: dict[int, str] = {}
    case_scrif_failure: str | None = None
    mode = str(q_mode).strip().lower()

    ok, msg = solve_case_safe_n(log_path, n=2)
    if not ok:
        IF0 = pd.DataFrame(index=buses, columns=buses, dtype=float)
        fail_msg = f"FAILED: {msg}"
        for j in buses:
            col_status[j] = fail_msg
            dq_records.append({
                "Case": case_name, "BusNumber": j,
                "Requested ΔQ (MVAr)": float(q_step_by_bus.get(j, DEFAULT_Q_STEP_MVAR)),
                "Actual ΔQ Used (MVAr)": np.nan,
                "Status": col_status[j]
            })
        return IF0, col_status, dq_records, msg

    v0 = get_bus_vpu(buses)
    IF = pd.DataFrame(index=buses, columns=buses, dtype=float)

    psspy.save(SCENARIO_TEMP)

    for j in buses:
        base_dq = float(q_step_by_bus.get(j, DEFAULT_Q_STEP_MVAR))
        if abs(base_dq) <= 0.0:
            IF.loc[:, j] = np.nan
            col_status[j] = "Q_STEP_ZERO_SKIPPED"
            dq_records.append({
                "Case": case_name,
                "BusNumber": j,
                "Requested ΔQ (MVAr)": base_dq,
                "Actual ΔQ Used (MVAr)": 0.0,
                "Status": col_status[j]
            })
            continue

        dq = base_dq
        success = False
        reason = "OK"

        while True:
            psspy.case(SCENARIO_TEMP)

            ok, msg = solve_case_safe_n(log_path, n=2)
            if not ok:
                reason = f"FAILED: {msg}"
                break

            vbase = get_bus_vpu(buses)
            max_drift = max(abs(vbase[b] - v0[b]) for b in buses) if buses else 0.0
            if max_drift > BASE_STATE_DRIFT_TOL:
                reason = f"BASE STATE DRIFT AFTER RELOAD (max ΔV={max_drift:.3e} pu)"
                break

            add_fixed_shunt_deltaQ(j, dq_mvar=dq, sh_id=SCRIF_SHUNT_ID)

            ok, msg = solve_case_safe_n(log_path, n=3)
            if not ok:
                reason = f"FAILED: {msg}"
                break

            v1 = get_bus_vpu(buses)
            dVj = v1[j] - vbase[j]

            if abs(dVj) >= DVJ_MIN:
                for i in buses:
                    IF.loc[i, j] = (v1[i] - vbase[i]) / dVj
                success = True
                reason = "OK" if dq == base_dq else f"OK (ΔQ={dq:g})"
                break

            if mode == "adaptive" and dq * ADAPTIVE_Q_GROWTH <= ADAPTIVE_Q_MAX_MVAR:
                dq *= ADAPTIVE_Q_GROWTH
                continue

            if mode == "fixed_skip":
                reason = f"ΔVj BELOW THRESHOLD (|ΔVj|<{DVJ_MIN:g})"
            else:
                reason = f"PERTURBATION TOO SMALL (max ΔQ={dq:g})"
            break

        if not success:
            IF.loc[:, j] = np.nan
            if (("PERTURBATION TOO SMALL" in reason) or ("BELOW THRESHOLD" in str(reason).upper())) and SKIP_WEAK_COLUMNS:
                col_status[j] = "WEAK COLUMN SKIPPED"
            else:
                col_status[j] = reason
                if case_scrif_failure is None and is_hard_scrif_failure(reason):
                    case_scrif_failure = reason
        else:
            col_status[j] = reason

        dq_records.append({
            "Case": case_name,
            "BusNumber": j,
            "Requested ΔQ (MVAr)": base_dq,
            "Actual ΔQ Used (MVAr)": dq,
            "Status": col_status[j]
        })

    for b in buses:
        if str(col_status.get(b, "")).startswith("OK"):
            IF.loc[b, b] = 1.0

    return IF, col_status, dq_records, case_scrif_failure


# ==========================================================
# SCR and SCRIF
# ==========================================================
def compute_scr(scmva: dict[int, float], p_mw: dict[int, float], buses: list[int]) -> dict[int, float]:
    out = {}
    for b in buses:
        P = float(p_mw.get(b, 0.0))
        S = scmva.get(b, np.nan)
        out[b] = (S / P) if (P > 0 and np.isfinite(S)) else np.nan
    return out

def compute_scrif_with_denominator(
    scmva: dict[int, float],
    IF: pd.DataFrame,
    p_mw: dict[int, float],
    buses: list[int],
) -> tuple[dict[int, float], dict[int, float]]:
    scrif_out = {}
    denom_out = {}
    for i in buses:
        Pi = float(p_mw.get(i, 0.0))
        denom = Pi
        for j in buses:
            if j == i:
                continue
            if_ij = IF.loc[i, j]
            if np.isnan(if_ij):
                continue
            denom += if_ij * float(p_mw.get(j, 0.0))
        denom_out[i] = denom if denom > 0 else np.nan
        scrif_out[i] = (scmva.get(i, np.nan) / denom) if denom > 0 else np.nan
    return scrif_out, denom_out


def compute_scrif(scmva: dict[int, float], IF: pd.DataFrame, p_mw: dict[int, float], buses: list[int]) -> dict[int, float]:
    scrif_out, _ = compute_scrif_with_denominator(scmva, IF, p_mw, buses)
    return scrif_out


# ==========================================================
# CONTINGENCY HELPERS
# ==========================================================
def normalize_component_id(raw_id) -> str:
    """Normalize Excel-sourced circuit/transformer IDs."""
    if pd.isna(raw_id):
        raise ValueError("Contingency ID is blank")
    if isinstance(raw_id, (int, np.integer)):
        return str(int(raw_id))
    if isinstance(raw_id, (float, np.floating)):
        if np.isnan(raw_id):
            raise ValueError("Contingency ID is blank")
        if float(raw_id).is_integer():
            return str(int(raw_id))
        return ("%g" % float(raw_id)).strip()
    s = str(raw_id).strip()
    if re.fullmatch(r"[+-]?\d+\.0+", s):
        return str(int(float(s)))
    return s



def _call_two_winding_outage_api(fb: int, tb: int, cid: str) -> tuple[str, int]:
    """
    Prefer the PSS/E 36 export-style API. Avoid hasattr(psspy, ...) because
    that can be unreliable with some PSSE Python bindings.

    Important: two_winding_chng_6 returns (ierr, realaro), not just ierr.
    """
    _i = psspy.getdefaultint()
    _f = psspy.getdefaultreal()
    _s = psspy.getdefaultchar()

    def _normalize_ierr(ret) -> int:
        if isinstance(ret, (tuple, list)):
            return int(ret[0])
        return int(ret)

    try:
        ret = psspy.two_winding_chng_6(
            fb, tb, cid,
            [0, _i, _i, _i, _i, _i, _i, _i, _i, _i, _i, _i, _i, _i, _i, _i],
            [_f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f,
             _f, _f, _f, _f, _f, _f, _f, _f, _f, _f],
            [_f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f],
            _s, _s
        )
        return "two_winding_chng_6", _normalize_ierr(ret)
    except AttributeError:
        pass

    try:
        ret = psspy.two_winding_chng_3(fb, tb, cid, intgar1=0)
        return "two_winding_chng_3", _normalize_ierr(ret)
    except AttributeError as e:
        raise RuntimeError(
            "Neither two_winding_chng_6 nor two_winding_chng_3 exists in this psspy module"
        ) from e

def outage_two_winding(fb: int, tb: int, cid: str) -> tuple[str, int, int]:
    """
    Outage a 2-winding transformer.
    - Uses the PSS/E 36 exported API form first.
    - Falls back to older API only if needed.
    - Retries with reversed bus order because transformer lookup in PSSE is
      often order-sensitive.
    Returns: (api_used, used_from_bus, used_to_bus)
    """
    cid = normalize_component_id(cid) if 'normalize_component_id' in globals() else str(cid).strip()
    attempts = [(int(fb), int(tb), cid)]
    if int(fb) != int(tb):
        attempts.append((int(tb), int(fb), cid))

    errors = []
    for a, b, tid in attempts:
        api_used, ierr = _call_two_winding_outage_api(a, b, tid)
        if ierr == 0:
            return api_used, a, b
        errors.append(f"{api_used}({a},{b},id={tid}) ierr={ierr}")

    raise RuntimeError(
        f"Transformer outage failed for original request {fb}-{tb} id={cid}. "
        + " ; ".join(errors)
    )

# ==========================================================
# CONTINGENCIES
# ==========================================================
def apply_contingency(row: pd.Series):
    t = str(row["Type"]).strip().lower()
    fb = int(row["FromNumber"])
    tb = int(row["ToNumber"])
    cid = normalize_component_id(row["ID"]) if 'normalize_component_id' in globals() else str(row["ID"]).strip()

    if t == "line":
        ierr = psspy.branch_chng_3(fb, tb, cid, intgar1=0)
        if ierr:
            raise RuntimeError(f"branch_chng_3 failed {fb}-{tb} id={cid} ierr={ierr}")
        return {
            "type": "line",
            "api": "branch_chng_3",
            "from_bus": fb,
            "to_bus": tb,
            "id": cid,
        }
    elif t == "transformer":
        api_used, used_fb, used_tb = outage_two_winding(fb, tb, cid)
        return {
            "type": "transformer",
            "api": api_used,
            "from_bus": used_fb,
            "to_bus": used_tb,
            "id": cid,
        }
    else:
        raise ValueError(f"Unknown contingency Type='{row['Type']}'")

def case_sort_key(x):
    s = str(x).strip()
    if s == "BASE":
        return -1
    m = re.fullmatch(r"Case-(\d+)", s)
    if m:
        return int(m.group(1))
    return 10**9


# ==========================================================
# MAIN
# ==========================================================
def main():
    df_bus, df_con, df_ded = read_inputs(INPUT_XLSX)

    buses = df_bus["BusNumber"].tolist()
    p_mw = dict(zip(df_bus["BusNumber"], df_bus["Installed Capacity"]))
    q_step_by_bus = dict(zip(df_bus["BusNumber"], df_bus["Q_step_MVAr"]))
    deduction_mva_by_bus = build_fault_mva_deduction_map(df_ded, buses)

    dq_table: list[dict] = []
    scrif_denom_table: list[dict] = []

    # ========== BASE ==========
    psspy.case(CASE_PATH)
    base_log = os.path.join(LOG_DIR, "base_case.log")

    with PsseLogCapture(base_log):
        ok, msg = solve_case_safe_n(base_log, n=2)
        if not ok:
            raise RuntimeError(f"Base case did not solve: {msg}")
        psspy.save(BASE_TEMP)

        base_scmva_raw = get_fault_mva_iec60909(buses)
        base_scmva = apply_fault_mva_deduction(base_scmva_raw, deduction_mva_by_bus, buses)
        base_scr = compute_scr(base_scmva, p_mw, buses)

        base_IF, base_status, base_dq, base_scrif_fail = compute_if_matrix(buses, q_step_by_bus, base_log, case_name="BASE")
        dq_table.extend(base_dq)

        if base_scrif_fail is not None:
            base_scrif = {b: base_scrif_fail for b in buses}
        else:
            base_scrif, base_scrif_denom = compute_scrif_with_denominator(base_scmva, base_IF, p_mw, buses)
            for b in buses:
                scrif_denom_table.append({
                    "Case": "BASE",
                    "BusNumber": b,
                    "SCRIF Denominator": base_scrif_denom.get(b, np.nan),
                })

    # Base columns for both sheets
    base_cols = ["BusNumber"]
    if "Station Name" in df_bus.columns:
        base_cols.append("Station Name")
    if "VoltageKV" in df_bus.columns:
        base_cols.append("VoltageKV")
    base_cols.append("Installed Capacity")
    base_df = df_bus[base_cols].copy()

    # Initialize sheet dataframes
    scr_df = base_df.copy()
    scr_df["Base Fault Level (MVA)"] = scr_df["BusNumber"].map(base_scmva)
    scr_df["Base SCR"] = scr_df["BusNumber"].map(base_scr)

    scrif_df = base_df.copy()
    scrif_df["Base Fault Level (MVA)"] = scrif_df["BusNumber"].map(base_scmva)
    scrif_df["Base SCRIF"] = scrif_df["BusNumber"].map(base_scrif)

    # ========== CONTINGENCIES ==========
    for _, row in df_con.iterrows():
        k = int(row["Sl No"])
        scr_col = f"Case-{k}"
        scrif_col = f"Case-{k} SCRIF"

        scr_df[scr_col] = np.nan
        scrif_df[scrif_col] = np.nan

        log_file = os.path.join(LOG_DIR, f"case_{k}.log")

        psspy.case(BASE_TEMP)

        with PsseLogCapture(log_file):
            ok, msg = solve_case_safe_n(log_file, n=2)

            if ok:
                applied = None
                try:
                    applied = apply_contingency(row)
                except Exception as e:
                    ok = False
                    msg = f"CONTINGENCY APPLY FAILED: {e}"

            # Solve 3 times after applying contingency
            if ok:
                ok, msg = solve_case_safe_n(log_file, n=3)

            if not ok:
                scr_df[scr_col] = msg
                scrif_df[scrif_col] = msg
                print(f"❌ Case-{k} FAILED: {msg}")
                continue

            # SCR
            c_scmva_raw = get_fault_mva_iec60909(buses)
            c_scmva = apply_fault_mva_deduction(c_scmva_raw, deduction_mva_by_bus, buses)
            c_scr = compute_scr(c_scmva, p_mw, buses)
            scr_df[scr_col] = scr_df["BusNumber"].map(c_scr)

            # SCRIF + ΔQ Used table
            c_IF, c_status, c_dq, c_scrif_fail = compute_if_matrix(buses, q_step_by_bus, log_file, case_name=f"Case-{k}")
            dq_table.extend(c_dq)

            if c_scrif_fail is not None:
                scrif_df[scrif_col] = c_scrif_fail
            else:
                c_scrif, c_scrif_denom = compute_scrif_with_denominator(c_scmva, c_IF, p_mw, buses)
                scrif_df[scrif_col] = scrif_df["BusNumber"].map(c_scrif)
                for b in buses:
                    scrif_denom_table.append({
                        "Case": f"Case-{k}",
                        "BusNumber": b,
                        "SCRIF Denominator": c_scrif_denom.get(b, np.nan),
                    })

        if ok:
            suffix = ""
            if isinstance(applied, dict):
                suffix = (
                    f" via {applied.get('api', '?')} on "
                    f"{applied.get('from_bus', row['FromNumber'])}-{applied.get('to_bus', row['ToNumber'])}"
                )
            print(
                f"Done Case-{k}: {row['Type']} {row['FromNumber']}-{row['ToNumber']} "
                f"ID={normalize_component_id(row['ID'])}{suffix}"
            )

    # ========== Append bottom contingency-description rows ==========
    scr_df["BusNumber"] = scr_df["BusNumber"].astype(object)
    scrif_df["BusNumber"] = scrif_df["BusNumber"].astype(object)

    desc_col = "Contingency" if "Contingency" in df_con.columns else None
    scr_rows, scrif_rows = [], []

    for _, row in df_con.iterrows():
        k = int(row["Sl No"])
        desc = str(row[
                       desc_col]).strip() if desc_col else f"{row['Type']} {row['FromNumber']}-{row['ToNumber']} ID={row['ID']}"

        r1 = {c: np.nan for c in scr_df.columns}
        r1["BusNumber"] = f"Case-{k}"
        if "Station Name" in scr_df.columns:
            r1["Station Name"] = desc
        scr_rows.append(r1)

        r2 = {c: np.nan for c in scrif_df.columns}
        r2["BusNumber"] = f"Case-{k}"
        if "Station Name" in scrif_df.columns:
            r2["Station Name"] = desc
        scrif_rows.append(r2)  # <--- FIXED

    if scr_rows:
        scr_df = pd.concat([scr_df, pd.DataFrame(scr_rows, columns=scr_df.columns)], ignore_index=True)

    if scrif_rows:
        scrif_df = pd.concat([scrif_df, pd.DataFrame(scrif_rows, columns=scrif_df.columns)], ignore_index=True)

    # ========== ΔQ Used Sheet ==========
    dq_df = pd.DataFrame(dq_table)
    dq_df["CaseSort"] = dq_df["Case"].apply(case_sort_key)
    dq_df = dq_df.sort_values(["CaseSort", "BusNumber"]).drop(columns=["CaseSort"])

    scrif_denom_df = pd.DataFrame(scrif_denom_table)
    if not scrif_denom_df.empty:
        scrif_denom_df["CaseSort"] = scrif_denom_df["Case"].apply(case_sort_key)
        scrif_denom_df = scrif_denom_df.sort_values(["CaseSort", "BusNumber"]).drop(columns=["CaseSort"])

    # ========== Save ==========
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        scr_df.to_excel(writer, sheet_name="SCR", index=False)
        scrif_df.to_excel(writer, sheet_name="SCRIF", index=False)
        if not scrif_denom_df.empty:
            scrif_denom_df.to_excel(writer, sheet_name="SCRIF Denominator", index=False)
        dq_df.to_excel(writer, sheet_name="ΔQ_Used", index=False)

    print(f"\n✅ Saved: {OUTPUT_XLSX} (Sheets: SCR, SCRIF, SCRIF Denominator, ΔQ_Used)")
    print(f"📝 PSSE logs saved in: {LOG_DIR}")


if __name__ == "__main__":
    main()