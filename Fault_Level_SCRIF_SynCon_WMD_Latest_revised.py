
import os
import re
import math
import time
import itertools
import numpy as np
import pandas as pd

# ---------------- PSS/E Imports ----------------
# -----------------------------------------------------------------------
# OS-LEVEL STDOUT/STDERR REDIRECT
# PSSE DLLs write directly to the Windows console via C-level printf /
# WriteFile(stdout), completely bypassing Python sys.stdout and the
# psspy.*_output() API.  The ONLY reliable way to suppress this is to
# redirect file-descriptor 1 (and 2) at the OS level with os.dup2()
# BEFORE any PSSE import or init.  We save the real fds and restore them
# after init so that our own print() calls still reach the terminal.
# -----------------------------------------------------------------------
import sys as _sys

_PSSE_SILENT_LOG = os.path.join(os.getcwd(), "psse_silent.log")

class _OsSilencer:
    """Context manager: redirect OS-level stdout & stderr to a file."""
    def __init__(self, path: str):
        self._path = path
        self._saved_out = None
        self._saved_err = None
        self._fh = None

    def __enter__(self):
        try:
            self._fh = open(self._path, "a", encoding="utf-8", errors="replace")
            self._saved_out = os.dup(1)
            self._saved_err = os.dup(2)
            os.dup2(self._fh.fileno(), 1)
            os.dup2(self._fh.fileno(), 2)
        except Exception:
            pass
        return self

    def __exit__(self, *_):
        try:
            _sys.stdout.flush()
            _sys.stderr.flush()
        except Exception:
            pass
        try:
            if self._saved_out is not None:
                os.dup2(self._saved_out, 1)
                os.close(self._saved_out)
            if self._saved_err is not None:
                os.dup2(self._saved_err, 2)
                os.close(self._saved_err)
        except Exception:
            pass
        try:
            if self._fh is not None:
                self._fh.close()
        except Exception:
            pass
        return False

def silent_psse_call(func, *args, **kwargs):
    """Run a PSSE API call with OS-level stdout/stderr silenced."""
    with _OsSilencer(_PSSE_SILENT_LOG):
        return func(*args, **kwargs)

# Import and init PSSE with all C-level output silenced
with _OsSilencer(_PSSE_SILENT_LOG):
    import psse36
    import psspy
    import pssarrays
    import redirect
    psspy.psseinit(50000)
    # Also tell PSSE's own report/progress/alert/prompt channels to go to file
    for _fn in (psspy.report_output, psspy.progress_output,
                psspy.alert_output, psspy.prompt_output):
        try:
            _fn(2, _PSSE_SILENT_LOG, [0, 0])
        except Exception:
            pass

# ==========================================================
# USER INPUTS
# ==========================================================
#CASE_PATH   = r"4_Bus_Test_System.sav"
#INPUT_XLSX  = r"input_4_bus_test_system_syncon.xlsx"
#OUTPUT_XLSX = r"SCR_SCRIF_results_4_bus_test_system_syncon.xlsx"

CASE_PATH   = r"23_Bus_Test_System.sav"
INPUT_XLSX  = r"input_23_bus_test_system_syncon.xlsx"
OUTPUT_XLSX = r"SCR_SCRIF_results_23_bus_test_system_syncon.xlsx"

# Optional: save the validated final optimal SynCon portfolio as a PSSE .sav case
SAVE_FINAL_OPTIMAL_CASE = True
FINAL_OPTIMAL_CASE_PATH = os.path.splitext(OUTPUT_XLSX)[0] + "_final_optimal_solution.sav"

# Power flow solve options
FNSL_OPTS = [0, 0, 0, 1, 0, 0, 0, 0]

# ΔQ settings for IF (default; can be overridden per bus in Excel)
DEFAULT_Q_STEP_MVAR = 10.0

# IF perturbation mode:
# - "adaptive": start with per-bus Q_step_MVAr and automatically increase ΔQ until |ΔVj| >= DVJ_MIN
# - "fixed_skip": use only the per-bus Q_step_MVAr once; if |ΔVj| < DVJ_MIN, skip the IF column
IF_Q_MODE = "fixed_skip"

# Minimum |ΔVj| threshold for IF normalization (avoid dividing by tiny number)
DVJ_MIN = 1e-6

# Perturbation handling
ADAPTIVE_Q_MAX_MVAR = 500.0
ADAPTIVE_Q_GROWTH = 2.0
SKIP_WEAK_COLUMNS = True
BASE_STATE_DRIFT_TOL = 1e-6

# Temp snapshots
BASE_TEMP     = os.path.join(os.getcwd(), "base_temp.sav")
SCENARIO_TEMP = os.path.join(os.getcwd(), "scenario_temp.sav")

# Dedicated fixed-shunt ID for perturbation
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
# OPTIMIZATION SETTINGS
# ==========================================================
ENABLE_SYNCON_OPTIMIZATION = True

# Optimization target
# "SCR" or "SCRIF"
OPT_TARGET_METRIC = "SCRIF"
OPT_MIN_THRESHOLD = 5.0

# Optimization mode:
# "FAST_HYBRID" -> actual one-by-one screening + exact search on shortlist
# "EXACT_FULL"  -> exact search on all candidates
OPTIMIZATION_MODE = "FAST_HYBRID"

# Fast hybrid controls
SHORTLIST_SIZE = 10
RANK_BY = "DEFICIT_REDUCTION"   # "DEFICIT_REDUCTION" or "PASSCOUNT_THEN_COST"
INCLUDE_ALREADY_FEASIBLE_BASE = False
MAX_EXACT_SHORTLIST = 16
PROGRESS_EVERY = 50

# Full exact controls
MAX_EXACT_FULL = 18

# Candidate count limit
OPT_MAX_SELECTED = None   # None or integer

# SynCon insertion settings
FORCE_CANDIDATE_BUS_TO_TYPE2 = True
SYNCON_PGEN_MW = 0.0
SYNCON_QGEN_INIT_MVAR = 0.0

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

def get_bus_type(bus: int) -> int:
    ierr, itype = _unwrap(psspy.busint(bus, "TYPE"))
    if ierr:
        raise RuntimeError(f"busint(TYPE) failed bus={bus} ierr={ierr}")
    return int(itype)

def bus_exists(bus: int) -> bool:
    try:
        ierr, _ = _unwrap(psspy.busdat(bus, "NUMBER"))
        return ierr == 0
    except Exception:
        return False


# ==========================================================
# --------- PSSE OUTPUT LOGGING + FAILURE CLASSIFY ---------
# ==========================================================
class PsseLogCapture:
    """
    Redirect PSSE internal output channels to a per-operation log file.

    Uses psspy.*_output(2, path) to redirect PSSE's report/progress/alert/
    prompt streams.  On exit, restores them to the global silent sink
    (NOT back to the console) so nothing leaks between operations.

    Note: C-level DLL output from iecs_currents() is handled separately
    via _OsSilencer applied only around that specific call, so that all
    Python print() progress messages remain visible on the console.
    """
    def __init__(self, log_path: str):
        self.log_path = log_path

    def __enter__(self):
        try:
            open(self.log_path, "w", encoding="utf-8").close()
        except Exception:
            pass
        for _fn in (psspy.report_output, psspy.progress_output,
                    psspy.alert_output, psspy.prompt_output):
            try:
                _fn(2, self.log_path, [0, 0])
            except Exception:
                pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore to the global silent sink — NOT to console (channel 6)
        for _fn in (psspy.report_output, psspy.progress_output,
                    psspy.alert_output, psspy.prompt_output):
            try:
                _fn(2, _PSSE_SILENT_LOG, [0, 0])
            except Exception:
                pass
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
    - Failure keywords are checked ONLY in the log tail corresponding to the LAST solve attempt
    - Optional mismatch hard fail
    """
    start = log_mark(log_path)
    with _OsSilencer(_PSSE_SILENT_LOG):
        ierr, _ = _unwrap(psspy.fnsl(FNSL_OPTS))

    if ierr != 0:
        try:
            with _OsSilencer(_PSSE_SILENT_LOG):
                psspy.fdns([0, 0, 0, 1, 1, 0, 99, 0])
            start = log_mark(log_path)
            with _OsSilencer(_PSSE_SILENT_LOG):
                ierr, _ = _unwrap(psspy.fnsl(FNSL_OPTS))
        except Exception:
            pass

    if ierr != 0:
        tail = read_log_upper_from(log_path, start)
        return False, classify_psse_failure(log_path, ierr=ierr, tail_txt_upper=tail)

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
CONTINGENCY_CANONICAL_COLUMNS = ["Sl No", "Type", "FromNumber", "ToNumber", "ID", "Contingency"]
DEVICE_CONTINGENCY_SHEET = "Machine_FACTS_Contingency"

def canonicalize_contingency_type(raw_type) -> str:
    t = str(raw_type).strip().lower()
    aliases = {
        "line": "line",
        "transformer": "transformer",
        "machine": "machine",
        "generator": "machine",
        "gen": "machine",
        "facts": "facts",
        "fact": "facts",
        "facts device": "facts",
        "factsdevice": "facts",
    }
    return aliases.get(t, t)

def empty_contingency_table() -> pd.DataFrame:
    return pd.DataFrame(columns=CONTINGENCY_CANONICAL_COLUMNS)

def read_optional_input_sheet(xlsx_path: str, sheet_name: str) -> pd.DataFrame | None:
    try:
        df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    except Exception:
        return None
    df.columns = df.columns.str.strip()
    return df

def prepare_contingency_table(df_raw: pd.DataFrame | None, sheet_name: str) -> pd.DataFrame:
    if df_raw is None:
        return empty_contingency_table()

    df = df_raw.copy()
    if df.empty:
        return empty_contingency_table()

    df.columns = df.columns.str.strip()
    df = df.dropna(how="all").copy()
    if df.empty:
        return empty_contingency_table()

    if "FromNumber" not in df.columns and "BusNumber" in df.columns:
        df["FromNumber"] = df["BusNumber"]
    if "ToNumber" not in df.columns:
        df["ToNumber"] = 0
    if "Contingency" not in df.columns:
        df["Contingency"] = ""

    need_con = {"Sl No", "Type", "FromNumber", "ToNumber", "ID"}
    missing = need_con - set(df.columns)
    if missing:
        raise ValueError(f"{sheet_name} sheet missing columns: {sorted(list(missing))}")

    df["Type"] = df["Type"].astype(str).str.strip()
    df = df[df["Type"].ne("") & df["Type"].str.lower().ne("nan")].copy()
    if df.empty:
        return empty_contingency_table()

    type_keys = df["Type"].map(canonicalize_contingency_type)
    supported_types = {"line", "transformer", "machine", "facts"}
    unsupported = sorted(set(df.loc[~type_keys.isin(supported_types), "Type"].tolist()))
    if unsupported:
        raise ValueError(
            f"{sheet_name} sheet has unsupported contingency Type values: {unsupported}. "
            f"Supported values are {sorted(supported_types)}."
        )

    df["Sl No"] = pd.to_numeric(df["Sl No"], errors="coerce")
    df["FromNumber"] = pd.to_numeric(df["FromNumber"], errors="coerce")
    df["ToNumber"] = pd.to_numeric(df["ToNumber"], errors="coerce")

    if df["Sl No"].isna().any():
        raise ValueError(f"{sheet_name} sheet has blank/invalid 'Sl No' values.")
    if df["FromNumber"].isna().any():
        raise ValueError(f"{sheet_name} sheet has blank/invalid bus numbers in 'FromNumber'/'BusNumber'.")

    network_mask = type_keys.isin({"line", "transformer"})
    if df.loc[network_mask, "ToNumber"].isna().any():
        raise ValueError(f"{sheet_name} sheet has blank/invalid 'ToNumber' values for line/transformer contingencies.")

    df.loc[~network_mask, "ToNumber"] = df.loc[~network_mask, "ToNumber"].fillna(0)
    df["ToNumber"] = df["ToNumber"].fillna(0)

    df["ID"] = df["ID"].fillna("").astype(str).str.strip()
    blank_id = df["ID"].eq("") | df["ID"].str.lower().eq("nan")
    if blank_id.any():
        raise ValueError(f"{sheet_name} sheet has blank/invalid contingency IDs.")

    df["Contingency"] = df["Contingency"].fillna("").astype(str).str.strip()

    type_display = {"line": "Line", "transformer": "Transformer", "machine": "Machine", "facts": "FACTS"}
    df["Type"] = type_keys.map(type_display)
    df["Sl No"] = df["Sl No"].astype(int)
    df["FromNumber"] = df["FromNumber"].astype(int)
    df["ToNumber"] = df["ToNumber"].astype(int)

    return df[CONTINGENCY_CANONICAL_COLUMNS].reset_index(drop=True)

def read_inputs(xlsx_path: str):
    df_bus = pd.read_excel(xlsx_path, sheet_name="Bus")
    df_con_main = pd.read_excel(xlsx_path, sheet_name="Contingency")
    df_con_device = read_optional_input_sheet(xlsx_path, DEVICE_CONTINGENCY_SHEET)

    df_bus.columns = df_bus.columns.str.strip()
    df_con_main.columns = df_con_main.columns.str.strip()

    need_bus = {"BusNumber", "Installed Capacity"}
    if not need_bus.issubset(df_bus.columns):
        raise ValueError(f"Bus sheet missing columns: {sorted(list(need_bus - set(df_bus.columns)))}")

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
        df_bus["Q_step_MVAr"] = df_bus["Q_step_MVAr"].fillna(0.0)

    # Optional MVA deduction sheet. If missing/incomplete, fall back safely to zero deduction.
    df_ded = pd.DataFrame(columns=["BusNumber", "MVA Capacity", "Percentage Deduction"])
    try:
        df_ded = pd.read_excel(xlsx_path, sheet_name="MVA_Deduction")
        df_ded.columns = df_ded.columns.str.strip()
    except Exception:
        df_ded = pd.DataFrame(columns=["BusNumber", "MVA Capacity", "Percentage Deduction"])

    df_con = prepare_contingency_table(df_con_main, "Contingency")
    df_con_device = prepare_contingency_table(df_con_device, DEVICE_CONTINGENCY_SHEET)

    if df_con.empty and df_con_device.empty:
        df_con = empty_contingency_table()
    else:
        df_con = pd.concat([df_con, df_con_device], ignore_index=True)
        dupes = sorted(df_con.loc[df_con["Sl No"].duplicated(keep=False), "Sl No"].unique().tolist())
        if dupes:
            raise ValueError(
                f"Duplicate contingency serial numbers found across 'Contingency' and "
                f"'{DEVICE_CONTINGENCY_SHEET}' sheets: {dupes}"
            )
        df_con = df_con.sort_values("Sl No", kind="stable").reset_index(drop=True)

    return df_bus, df_con, df_ded

def read_syncon_inputs(xlsx_path: str) -> pd.DataFrame | None:
    """
    Supported required columns:
      Candidate, CandidateBus, Cost, MBASE_MVA, QMAX_MVAr, QMIN_MVAr, Xsource_pu, MachID
    Supported optional columns:
      Eligible, Pgen, Pmax, Pmin

    Notes:
      - If Pgen/Pmax/Pmin are absent or blank, the legacy SynCon behavior is preserved.
      - Pgen defaults to SYNCON_PGEN_MW.
      - Pmax/Pmin default to Pgen.
    """
    try:
        df_syn = pd.read_excel(xlsx_path, sheet_name="SynCon")
    except Exception:
        return None

    df_syn.columns = df_syn.columns.str.strip()
    need_syn = {"CandidateBus", "Cost", "MBASE_MVA", "QMAX_MVAr", "QMIN_MVAr", "Xsource_pu", "MachID"}
    missing = need_syn - set(df_syn.columns)
    if missing:
        raise ValueError(f"SynCon sheet missing columns: {sorted(list(missing))}")

    if "Eligible" in df_syn.columns:
        df_syn = df_syn[df_syn["Eligible"].fillna(0).astype(float) > 0].copy()
    else:
        df_syn = df_syn.copy()

    if df_syn.empty:
        return df_syn

    if "Candidate" not in df_syn.columns:
        df_syn["Candidate"] = [f"SC{i:02d}" for i in range(1, len(df_syn) + 1)]
    else:
        df_syn["Candidate"] = df_syn["Candidate"].astype(str).str.strip()

    df_syn["CandidateBus"] = df_syn["CandidateBus"].astype(int)
    df_syn["Cost"] = df_syn["Cost"].astype(float)
    df_syn["MBASE_MVA"] = df_syn["MBASE_MVA"].astype(float)
    df_syn["QMAX_MVAr"] = df_syn["QMAX_MVAr"].astype(float)
    df_syn["QMIN_MVAr"] = df_syn["QMIN_MVAr"].astype(float)
    df_syn["Xsource_pu"] = df_syn["Xsource_pu"].astype(float)
    df_syn["MachID"] = df_syn["MachID"].astype(str).str.strip().str.upper()

    if "Pgen" not in df_syn.columns:
        df_syn["Pgen"] = SYNCON_PGEN_MW
    else:
        df_syn["Pgen"] = pd.to_numeric(df_syn["Pgen"], errors="coerce").fillna(SYNCON_PGEN_MW)

    if "Pmax" not in df_syn.columns:
        df_syn["Pmax"] = df_syn["Pgen"]
    else:
        df_syn["Pmax"] = pd.to_numeric(df_syn["Pmax"], errors="coerce").fillna(df_syn["Pgen"])

    if "Pmin" not in df_syn.columns:
        df_syn["Pmin"] = df_syn["Pgen"]
    else:
        df_syn["Pmin"] = pd.to_numeric(df_syn["Pmin"], errors="coerce").fillna(df_syn["Pgen"])

    df_syn["Pgen"] = df_syn["Pgen"].astype(float)
    df_syn["Pmax"] = df_syn["Pmax"].astype(float)
    df_syn["Pmin"] = df_syn["Pmin"].astype(float)

    if (df_syn["Xsource_pu"] <= 0).any():
        raise ValueError("All Xsource_pu values must be > 0")
    if (df_syn["Pmax"] < df_syn["Pmin"]).any():
        dups = df_syn.loc[df_syn["Pmax"] < df_syn["Pmin"], ["Candidate", "CandidateBus", "Pmax", "Pmin"]]
        raise ValueError("Pmax must be >= Pmin for all SynCon rows:\n" + dups.to_string(index=False))
    if df_syn["Candidate"].duplicated().any():
        dups = df_syn.loc[df_syn["Candidate"].duplicated(keep=False), "Candidate"].tolist()
        raise ValueError(f"Duplicate Candidate values found: {dups}")
    if (df_syn["MachID"].str.len() == 0).any():
        raise ValueError("MachID cannot be blank")
    if (df_syn["MachID"].str.len() > 2).any():
        raise ValueError("MachID must be at most 2 characters for PSSE")
    if df_syn.duplicated(subset=["CandidateBus", "MachID"]).any():
        dups = df_syn.loc[df_syn.duplicated(subset=["CandidateBus", "MachID"], keep=False), ["Candidate", "CandidateBus", "MachID"]]
        raise ValueError("Duplicate (CandidateBus, MachID) pairs found:\n" + dups.to_string(index=False))

    return df_syn.reset_index(drop=True)


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
    fltbus = list(getattr(rlst, "fltbus", []))
    flt3ph = getattr(rlst, "flt3ph", None)

    if hasattr(flt3ph, "values"):
        values = list(flt3ph.values())
        if len(values) == len(fltbus):
            for bus_number, fault_data in zip(fltbus, values):
                yield bus_number, fault_data
            return

    if hasattr(flt3ph, "get"):
        for bus_number in fltbus:
            fault_data = flt3ph.get(bus_number, None)
            if fault_data is None:
                fault_data = flt3ph.get(str(bus_number), None)
            yield bus_number, fault_data
        return

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
    _iecs_log = os.path.join(LOG_DIR, "_iecs_scratch.log")

    set_bus_subsystem(1, buses)
    apply_iec_gui_settings_for_selected_buses()

    # Silence all console output only for the IEC short-circuit run.
    # This suppresses the large bus-wise fault current tables produced by
    # pssarrays.iecs_currents() while keeping our own progress print() output visible.
    with _OsSilencer(_iecs_log):
        for _fn in (psspy.report_output, psspy.progress_output,
                    psspy.alert_output, psspy.prompt_output):
            try:
                _fn(2, _iecs_log, [0, 0])
            except Exception:
                pass

        rlst = pssarrays.iecs_currents(sid=1, all=0, flt3ph=1, cfactor=3, ucfactor=1.0)

    # Restore PSSE channels back to the global silent sink after IEC run.
    for _fn in (psspy.report_output, psspy.progress_output,
                psspy.alert_output, psspy.prompt_output):
        try:
            _fn(2, _PSSE_SILENT_LOG, [0, 0])
        except Exception:
            pass

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
    with _OsSilencer(_PSSE_SILENT_LOG):
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

    with _OsSilencer(_PSSE_SILENT_LOG):
        psspy.save(SCENARIO_TEMP)

    t_if_start = time.time()
    n_if = len(buses)
    progress_every_if = max(1, min(10, n_if // 20 if n_if >= 20 else 1))

    for idx_j, j in enumerate(buses, start=1):
        if (idx_j == 1) or (idx_j % progress_every_if == 0) or (idx_j == n_if):
            elapsed = time.time() - t_if_start
            rate = idx_j / max(elapsed, 1e-9)
            eta = (n_if - idx_j) / max(rate, 1e-9)
            print(f"[IF:{case_name}] {idx_j}/{n_if} | elapsed {format_eta(elapsed)} | ETA {format_eta(eta)}")
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
            with _OsSilencer(_PSSE_SILENT_LOG):
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

def compute_scrif(scmva: dict[int, float], IF: pd.DataFrame, p_mw: dict[int, float], buses: list[int]) -> dict[int, float]:
    out = {}
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
        out[i] = (scmva.get(i, np.nan) / denom) if denom > 0 else np.nan
    return out


# ==========================================================
# SYNCON MODELING
# ==========================================================
def _ensure_bus_type2(bus: int):
    if not FORCE_CANDIDATE_BUS_TO_TYPE2:
        return
    current_type = get_bus_type(bus)
    if current_type == 2:
        return

    _i = psspy.getdefaultint()
    _f = psspy.getdefaultreal()
    _s = psspy.getdefaultchar()

    ierr = 1
    try:
        ierr = psspy.bus_chng_4(
            bus,
            0,
            [2, _i, _i, _i],
            [_f, _f, _f, _f, _f, _f, _f],
            _s
        )
    except Exception:
        ierr = 1

    if ierr != 0:
        raise RuntimeError(f"Unable to convert bus {bus} to TYPE-2 (ierr={ierr})")

    new_type = get_bus_type(bus)
    if new_type != 2:
        raise RuntimeError(f"Bus {bus} type change failed. Current type={new_type}, expected 2.")

def _normalize_psse_ierr(ret):
    """Return only ierr from PSSE calls that may return int or (ierr, value)."""
    if isinstance(ret, (tuple, list)):
        return int(ret[0])
    return int(ret)


def _try_create_plant(bus: int):
    try:
        vset = max(get_bus_pu(bus), 1.0)
    except Exception:
        vset = 1.0

    # Prefer the PSSE 36 API family captured by the manual SynCon-add script.
    for creator in (
        lambda: psspy.plant_data_4(bus, 0, [0, 0], [vset, 100.0]),
        lambda: psspy.plant_data_3(bus, 0, [0], [vset, 100.0, 100.0, 100.0]),
        lambda: psspy.plant_data(bus, 0, [vset, 100.0]),
    ):
        try:
            ierr = _normalize_psse_ierr(creator())
            if ierr == 0:
                return
        except Exception:
            pass


def add_syncon_to_case(row: pd.Series):
    """
    Add a synchronous condenser candidate to the current PSSE case.

    Key resolution from the diagnostic logs:
    - machine_data_5 succeeds
    - machine_chng_5 succeeds and writes ZX = Xsource_pu
    - seq_machine_data_4 then fails with "Data Requirements not met (006835)"

    In this model, IEC 60909 is already using machine R/X from the fault-analysis
    data block after machine_chng_5. Therefore seq_machine_data_4 is treated as a
    best-effort enhancement, not a fatal requirement.
    """
    bus = int(row["CandidateBus"])
    mach_id = str(row["MachID"]).strip().upper()[:2]
    qmax = float(row["QMAX_MVAr"])
    qmin = float(row["QMIN_MVAr"])
    pgen = float(row.get("Pgen", SYNCON_PGEN_MW))
    pmax = float(row.get("Pmax", pgen))
    pmin = float(row.get("Pmin", pgen))
    mbase = float(row["MBASE_MVA"])
    xsrc = float(row["Xsource_pu"])

    if xsrc <= 0:
        raise ValueError(f"Xsource_pu must be > 0 for bus={bus}, mach_id={mach_id}")

    _ensure_bus_type2(bus)
    _try_create_plant(bus)

    _i = psspy.getdefaultint()
    _f = psspy.getdefaultreal()
    _s = psspy.getdefaultchar()

    # Step 1: create the machine record.
    ierr = _normalize_psse_ierr(psspy.machine_data_5(
        bus,
        mach_id,
        [1, 1000, 0, 0, 0, 0, 0],
        [
            pgen, SYNCON_QGEN_INIT_MVAR,
            qmax, qmin,
            pmax, pmin,
            mbase,
            0.0, 1.0, 0.0, 0.0,
            1.0, 1.0, 1.0, 1.0, 1.0, 1.0,
        ],
        ["", ""],
    ))
    if ierr != 0:
        raise RuntimeError(f"machine_data_5 failed for bus={bus}, mach_id={mach_id}, ierr={ierr}")

    # Step 2: set operating limits and positive-sequence source reactance.
    ierr = _normalize_psse_ierr(psspy.machine_chng_5(
        bus,
        mach_id,
        [_i, _i, _i, _i, _i, _i, _i],
        [
            _f, _f,
            qmax, qmin,
            pmax, pmin,
            mbase,
            _f,
            xsrc,
            _f, _f, _f, _f, _f, _f, _f, _f,
        ],
        [_s, _s],
    ))
    if ierr != 0:
        raise RuntimeError(
            f"machine_chng_5 failed for bus={bus}, mach_id={mach_id}, ierr={ierr}. "
            f"Positive-sequence reactance ZX was not applied."
        )

    # Step 3: try to set sequence data, but do not fail the candidate if PSSE
    # rejects it with the known 006835/"Data Requirements not met" condition.
    # The logs show machine_data_5 + machine_chng_5 already succeed and write ZX.
    seq_ierr = _normalize_psse_ierr(psspy.seq_machine_data_4(
        bus,
        mach_id,
        _i,
        [_f, xsrc, _f, _f, _f, _f, xsrc, xsrc, _f, _f, _f],
    ))
    if seq_ierr != 0:
        print(
            f"WARNING: seq_machine_data_4 failed for bus={bus}, mach_id={mach_id}, ierr={seq_ierr}. "
            f"Continuing because machine_data_5/machine_chng_5 already created the SynCon and set ZX={xsrc}."
        )

def add_syncon_subset_to_case(df_sel: pd.DataFrame):
    for _, row in df_sel.iterrows():
        add_syncon_to_case(row)


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

def normalize_machine_id(raw_id) -> str:
    mach_id = normalize_component_id(raw_id)
    if len(mach_id) > 2:
        raise ValueError(f"Machine contingency ID '{mach_id}' is longer than 2 characters.")
    return mach_id

def normalize_label_key(raw_value) -> str:
    return re.sub(r"\s+", " ", str(raw_value).strip()).upper()

def resolve_facts_name(raw_name, send_bus: int | None = None) -> str:
    requested_key = normalize_label_key(raw_name)
    if not requested_key or requested_key == "NAN":
        raise ValueError("FACTS contingency name is blank")

    ierr_name, names = psspy.afactschar(-1, 3, 2, 3, ["FACTSNAME"])
    ierr_bus, send_numbers = psspy.afactsint(-1, 3, 2, 3, ["SENDNUMBER"])
    if ierr_name or ierr_bus:
        raise RuntimeError(f"Unable to query FACTS devices (ierr={ierr_name},{ierr_bus})")

    matches = []
    for exact_name, fb in zip(names[0], send_numbers[0]):
        if normalize_label_key(exact_name) != requested_key:
            continue
        if send_bus is not None and int(fb) != int(send_bus):
            continue
        matches.append(exact_name)

    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise RuntimeError(
            f"FACTS contingency '{raw_name}' matched multiple devices. "
            "Please specify a unique device name/bus combination."
        )

    available = [
        f"{re.sub(r'\\s+', ' ', str(exact_name).strip())} @ bus {int(fb)}"
        for exact_name, fb in zip(names[0], send_numbers[0])
    ]
    if send_bus is None:
        raise RuntimeError(f"FACTS device '{raw_name}' not found. Available devices: {available}")
    raise RuntimeError(
        f"FACTS device '{raw_name}' not found at bus {send_bus}. "
        f"Available devices: {available}"
    )

def outage_machine(bus: int, raw_id) -> str:
    mach_id = normalize_machine_id(raw_id)
    _i = psspy.getdefaultint()
    _f = psspy.getdefaultreal()
    _s = psspy.getdefaultchar()
    ierr = _normalize_psse_ierr(psspy.machine_chng_5(
        int(bus),
        mach_id,
        [0, _i, _i, _i, _i, _i, _i],
        [_f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f, _f],
        [_s, _s],
    ))
    if ierr:
        raise RuntimeError(f"machine_chng_5 failed for bus={bus} id={mach_id} ierr={ierr}")
    return mach_id

def outage_facts_device(send_bus: int, raw_name) -> str:
    facts_name = resolve_facts_name(raw_name, send_bus)
    ierr = _normalize_psse_ierr(psspy.purgfacts(facts_name))
    if ierr:
        raise RuntimeError(f"purgfacts failed for FACTS '{raw_name}' at bus={send_bus} ierr={ierr}")
    return re.sub(r"\s+", " ", str(facts_name).strip())

def default_contingency_description(row: pd.Series) -> str:
    t = canonicalize_contingency_type(row["Type"])
    fb = int(row["FromNumber"])
    tb = int(row["ToNumber"])
    cid = str(row["ID"]).strip()
    if t == "line":
        return f"Line outage {fb}-{tb} ID={cid}"
    if t == "transformer":
        return f"Transformer outage {fb}-{tb} ID={cid}"
    if t == "machine":
        return f"Machine trip at bus {fb} ID={cid}"
    if t == "facts":
        return f"FACTS trip '{cid}' at bus {fb}"
    return f"{row['Type']} {fb}-{tb} ID={cid}"



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
    t = canonicalize_contingency_type(row["Type"])
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
    elif t == "machine":
        mach_id = outage_machine(fb, cid)
        return {
            "type": "machine",
            "api": "machine_chng_5",
            "from_bus": fb,
            "to_bus": 0,
            "id": mach_id,
        }
    elif t == "facts":
        facts_name = outage_facts_device(fb, cid)
        return {
            "type": "facts",
            "api": "purgfacts",
            "from_bus": fb,
            "to_bus": 0,
            "id": facts_name,
        }
    else:
        raise ValueError(f"Unknown contingency Type='{row['Type']}'")

# ==========================================================
# OPTIMIZATION HELPERS
# ==========================================================
def metric_to_detail_rows(metric: dict[int, float], threshold: float, buses: list[int]) -> tuple[bool, pd.DataFrame]:
    rows = []
    all_ok = True
    total_deficit = 0.0
    pass_count = 0
    for b in buses:
        v = metric.get(b, np.nan)
        sat = int(np.isfinite(v) and v >= threshold)
        if sat:
            pass_count += 1
        else:
            all_ok = False
        deficit = 0.0 if not np.isfinite(v) else max(0.0, threshold - v)
        total_deficit += deficit
        rows.append({
            "BusNumber": b,
            "MetricValue": v,
            "Threshold": threshold,
            "Deficit": deficit,
            "Satisfied": sat,
        })
    return all_ok, pd.DataFrame(rows)

def evaluate_current_case_metrics(
    monitored_buses: list[int],
    p_mw: dict[int, float],
    base_if: pd.DataFrame,
    target_metric: str,
    q_step_by_bus: dict[int, float] | None = None,
    log_path: str | None = None,
    deduction_mva_by_bus: dict[int, float] | None = None,
) -> dict[int, float]:
    scmva_raw = get_fault_mva_iec60909(monitored_buses)
    scmva = apply_fault_mva_deduction(scmva_raw, deduction_mva_by_bus or {}, monitored_buses)
    scr = compute_scr(scmva, p_mw, monitored_buses)
    if str(target_metric).strip().upper() == "SCR":
        return scr
    elif str(target_metric).strip().upper() == "SCRIF":
        # For SCRIF we must recompute the IF matrix in the current (modified) case
        # so that SynCon reactive support is reflected in the interaction factors.
        if q_step_by_bus is not None and log_path is not None:
            new_if, _, _, fail = compute_if_matrix(
                buses=monitored_buses,
                q_step_by_bus=q_step_by_bus,
                log_path=log_path,
                case_name="OPT_EVAL",
            )
            if_matrix = new_if if fail is None else base_if
        else:
            # Fallback: use base IF matrix (acceptable for SCR-targeted runs)
            if_matrix = base_if
        scrif = compute_scrif(scmva, if_matrix, p_mw, monitored_buses)
        return scrif
    else:
        raise ValueError("target_metric must be SCR or SCRIF")


def format_eta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def global_benefit_score(metric: dict[int, float] | None, buses: list[int]) -> float:
    """Aggregate post-SynCon benefit score used for feasible-solution tie-breaks.

    For a fixed base case, maximizing the sum of post-installation target metrics
    across all monitored buses is equivalent to maximizing total global benefit.
    """
    if not metric:
        return -np.inf
    vals = []
    for b in buses:
        v = metric.get(b, np.nan)
        vals.append(v if np.isfinite(v) else -1e12)
    return float(np.sum(vals))


def evaluate_subset(
    subset_df: pd.DataFrame,
    monitored_buses: list[int],
    p_mw: dict[int, float],
    base_if: pd.DataFrame,
    target_metric: str,
    threshold: float,
    log_path: str,
    q_step_by_bus: dict[int, float] | None = None,
    deduction_mva_by_bus: dict[int, float] | None = None,
) -> tuple[bool, float, dict[int, float] | None, str, pd.DataFrame]:
    with _OsSilencer(_PSSE_SILENT_LOG):
        psspy.case(BASE_TEMP)

    try:
        add_syncon_subset_to_case(subset_df)
    except Exception as e:
        return False, np.inf, None, f"ADD FAILED: {e}", pd.DataFrame()

    ok, msg = solve_case_safe_n(log_path, n=3)
    if not ok:
        return False, np.inf, None, msg, pd.DataFrame()

    metric = evaluate_current_case_metrics(
        monitored_buses=monitored_buses,
        p_mw=p_mw,
        base_if=base_if,
        target_metric=target_metric,
        q_step_by_bus=q_step_by_bus,
        log_path=log_path,
        deduction_mva_by_bus=deduction_mva_by_bus,
    )
    feasible, det_df = metric_to_detail_rows(metric, threshold, monitored_buses)
    cost = float(subset_df["Cost"].sum()) if not subset_df.empty else 0.0
    return feasible, cost, metric, "OK", det_df

def screen_candidates_one_by_one(
    df_syn: pd.DataFrame,
    monitored_buses: list[int],
    p_mw: dict[int, float],
    base_if: pd.DataFrame,
    base_metric: dict[int, float],
    target_metric: str,
    threshold: float,
    log_path: str,
    q_step_by_bus: dict[int, float] | None = None,
    deduction_mva_by_bus: dict[int, float] | None = None,
) -> pd.DataFrame:
    rows = []
    _, base_det = metric_to_detail_rows(base_metric, threshold, monitored_buses)
    base_total_deficit = float(base_det["Deficit"].sum())
    base_pass_count = int(base_det["Satisfied"].sum())

    print("\nScreening candidates one by one in actual PSSE case...")
    t0 = time.time()

    for idx, (_, row) in enumerate(df_syn.iterrows(), start=1):
        subset_df = pd.DataFrame([row])
        feasible, cost, metric, status, det_df = evaluate_subset(
            subset_df=subset_df,
            monitored_buses=monitored_buses,
            p_mw=p_mw,
            base_if=base_if,
            target_metric=target_metric,
            threshold=threshold,
            log_path=log_path,
            q_step_by_bus=q_step_by_bus,
            deduction_mva_by_bus=deduction_mva_by_bus,
        )

        if metric is None or det_df.empty:
            total_deficit = np.inf
            pass_count = -1
        else:
            total_deficit = float(det_df["Deficit"].sum())
            pass_count = int(det_df["Satisfied"].sum())

        deficit_reduction = base_total_deficit - total_deficit if np.isfinite(total_deficit) else -np.inf

        rows.append({
            "Candidate": row["Candidate"],
            "CandidateBus": int(row["CandidateBus"]),
            "MachID": row["MachID"],
            "Cost": float(row["Cost"]),
            "FeasibleAlone": int(feasible),
            "PassCount": pass_count,
            "BasePassCount": base_pass_count,
            "TotalDeficitAlone": total_deficit,
            "BaseTotalDeficit": base_total_deficit,
            "DeficitReduction": deficit_reduction,
            "Status": status,
        })

        if idx % max(1, min(5, len(df_syn))) == 0 or idx == len(df_syn):
            elapsed = time.time() - t0
            rate = idx / max(elapsed, 1e-9)
            eta = (len(df_syn) - idx) / max(rate, 1e-9)
            print(f"[screen] {idx}/{len(df_syn)} | elapsed {format_eta(elapsed)} | ETA {format_eta(eta)}")

    out = pd.DataFrame(rows)
    if RANK_BY.upper() == "DEFICIT_REDUCTION":
        out = out.sort_values(
            by=["FeasibleAlone", "DeficitReduction", "PassCount", "Cost"],
            ascending=[False, False, False, True]
        ).reset_index(drop=True)
    else:
        out = out.sort_values(
            by=["FeasibleAlone", "PassCount", "Cost", "DeficitReduction"],
            ascending=[False, False, True, False]
        ).reset_index(drop=True)
    return out

def exact_enumerate_candidates(
    df_candidates: pd.DataFrame,
    monitored_buses: list[int],
    p_mw: dict[int, float],
    base_if: pd.DataFrame,
    target_metric: str,
    threshold: float,
    log_path: str,
    max_selected: int | None = None,
    exact_limit: int = 16,
    q_step_by_bus: dict[int, float] | None = None,
    deduction_mva_by_bus: dict[int, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df_candidates.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    n = len(df_candidates)
    if n > exact_limit:
        raise RuntimeError(f"Candidate count {n} exceeds exact limit {exact_limit}")

    indices = list(df_candidates.index)
    max_r = n if max_selected is None else min(n, int(max_selected))
    # Start from r=1: the empty-set (r=0) is never a valid solution
    total_subsets = sum(math.comb(n, r) for r in range(1, max_r + 1))

    print(f"\n[search] exact enumeration on {n} candidates | total subsets = {total_subsets}")

    best_cost = np.inf
    best_subset_idx = None
    best_metric = None
    best_global_benefit = -np.inf
    records = []

    count = 0
    t0 = time.time()

    for r in range(1, max_r + 1):
        for comb in itertools.combinations(indices, r):
            count += 1
            subset_df = df_candidates.loc[list(comb)].copy()

            feasible, cost, metric, status, det_df = evaluate_subset(
                subset_df=subset_df,
                monitored_buses=monitored_buses,
                p_mw=p_mw,
                base_if=base_if,
                target_metric=target_metric,
                threshold=threshold,
                log_path=log_path,
                q_step_by_bus=q_step_by_bus,
                deduction_mva_by_bus=deduction_mva_by_bus,
            )

            global_benefit = global_benefit_score(metric, monitored_buses) if feasible else np.nan

            records.append({
                "SubsetSize": r,
                "Candidates": ", ".join(subset_df["Candidate"].tolist()),
                "TotalCost": cost if np.isfinite(cost) else np.nan,
                "Feasible": int(feasible),
                "GlobalBenefit": global_benefit,
                "Status": status,
            })

            if feasible:
                if cost < best_cost - 1e-9:
                    best_cost = cost
                    best_subset_idx = list(comb)
                    best_metric = metric
                    best_global_benefit = global_benefit
                elif abs(cost - best_cost) <= 1e-9 and best_subset_idx is not None:
                    if len(comb) < len(best_subset_idx):
                        best_subset_idx = list(comb)
                        best_metric = metric
                        best_global_benefit = global_benefit
                    elif len(comb) == len(best_subset_idx) and global_benefit > best_global_benefit + 1e-9:
                        best_subset_idx = list(comb)
                        best_metric = metric
                        best_global_benefit = global_benefit

            if count % max(1, PROGRESS_EVERY) == 0 or count == total_subsets:
                elapsed = time.time() - t0
                rate = count / max(elapsed, 1e-9)
                eta = (total_subsets - count) / max(rate, 1e-9)
                current_names = subset_df["Candidate"].tolist()
                print(f"[search] {count}/{total_subsets} | elapsed {format_eta(elapsed)} | ETA {format_eta(eta)} | size={len(current_names)}")

    trace_df = pd.DataFrame(records)

    if best_subset_idx is None:
        return pd.DataFrame(), pd.DataFrame(), trace_df

    selected_df = df_candidates.loc[best_subset_idx].copy().reset_index(drop=True)
    achieved_rows = []
    for b in monitored_buses:
        achieved_rows.append({
            "BusNumber": b,
            "FinalMetric": best_metric.get(b, np.nan),
            "Threshold": threshold,
            "Satisfied": int(np.isfinite(best_metric.get(b, np.nan)) and best_metric.get(b, np.nan) >= threshold),
        })
    achieved_df = pd.DataFrame(achieved_rows)
    return selected_df, achieved_df, trace_df


# ==========================================================
# CLEAN RESULTS SUMMARY BUILDER
# ==========================================================
def build_results_summary(
    scr_df: pd.DataFrame,
    scrif_df: pd.DataFrame,
    selected_df: pd.DataFrame,
    achieved_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    screening_df: pd.DataFrame,
    base_metric_detail: pd.DataFrame,
    threshold: float,
    target_metric: str,
) -> pd.DataFrame:
    """
    Build a single, human-readable 'Results_Summary' sheet that consolidates:
      Section A  – Optimization outcome (what was decided and why)
      Section B  – Base case metric status per bus (pass / fail / value)
      Section C  – Selected SynCon portfolio (which ones, where, cost)
      Section D  – SCR improvement per bus after SynCon installation
      Section E  – Key statistics (feasibility, total cost, buses improved)
    """
    rows = []

    def _sep(title=""):
        rows.append({"#": "---", "Item": title, "Value": "", "Notes": ""})

    def _row(num, item, value, notes=""):
        rows.append({"#": num, "Item": item, "Value": value, "Notes": notes})

    # ── Section A: Optimization Outcome ─────────────────────────────────────
    _sep("SECTION A: OPTIMIZATION OUTCOME")
    if not summary_df.empty:
        s = summary_df.iloc[0]
        _row("A1", "Optimization Mode",       str(s.get("OptimizationMode", "")), "")
        _row("A2", "Target Metric",            str(s.get("TargetMetric", "")),     "SCR or SCRIF")
        _row("A3", "Minimum Threshold",        float(s.get("Threshold", np.nan)),  "Buses must meet this value")
        _row("A4", "Base Already Feasible?",   "YES" if int(s.get("BaseAlreadyFeasible", 0)) else "NO", "All buses above threshold before SynCon")
        _row("A5", "Total Candidates Provided",int(s.get("OriginalCandidateCount", 0)), "SynCon buses in input file")
        _row("A6", "Search Set Size",          int(s.get("SearchSetCount", 0)),    "Candidates evaluated in exact search")
        _row("A7", "SynCons Selected",         int(s.get("SelectedCount", 0)),     "Minimum-cost feasible portfolio found")
        _row("A8", "Total Portfolio Cost",     s.get("TotalCost", np.nan),         "Sum of Cost column for selected SynCons")
        _row("A9", "Validation Status",        str(s.get("ValidationStatus", "")), "Re-verified after selection")
        _row("A10","Validation Feasible?",     "YES" if int(s.get("ValidationFeasible", 0)) else "NO", "All buses pass after installing portfolio")
        gb = s.get("GlobalBenefit", np.nan)
        gb_str = f"{float(gb):.6f}" if pd.notna(gb) and np.isfinite(float(gb)) else ""
        _row("A11","Global Benefit", gb_str, "Sum of post-SynCon target metric across all monitored buses; used as tertiary tie-break")
    else:
        _row("A1", "Optimization", "NOT RUN", "ENABLE_SYNCON_OPTIMIZATION=False or no SynCon input")

    # ── Section B: Base Case – Bus-by-Bus Status ─────────────────────────────
    _sep("SECTION B: BASE CASE BUS STATUS  (threshold = " + str(threshold) + ")")
    _row("", "BusNumber", "Station Name", f"Base {target_metric.upper()}  |  Status  |  Deficit")
    if not base_metric_detail.empty:
        for _, r in base_metric_detail.iterrows():
            bus = int(r["BusNumber"])
            val = r["MetricValue"]
            sat = int(r.get("Satisfied", 0))
            deficit = float(r.get("Deficit", 0.0))
            # Look up station name from scr_df
            name_mask = scr_df["BusNumber"] == bus
            name = scr_df.loc[name_mask, "Station Name"].values[0] if (name_mask.any() and "Station Name" in scr_df.columns) else ""
            status = "✔ PASS" if sat else "✘ FAIL"
            val_str = f"{val:.3f}" if np.isfinite(float(val)) else "N/A"
            deficit_str = f"{deficit:.4f}" if deficit > 0 else "-"
            _row(f"B{bus}", str(bus), name, f"{val_str}  |  {status}  |  {deficit_str}")
    fail_count = int((~base_metric_detail["Satisfied"].astype(bool)).sum()) if not base_metric_detail.empty else 0
    pass_count = len(base_metric_detail) - fail_count if not base_metric_detail.empty else 0
    _row("B-Total", f"Buses Passing: {pass_count} / {len(base_metric_detail)}", f"Buses Failing: {fail_count}", "Before any SynCon")

    # ── Section C: Selected Portfolio ────────────────────────────────────────
    _sep("SECTION C: SELECTED SYNCON PORTFOLIO")
    if not selected_df.empty:
        _row("", "Candidate", "Bus | Station Name", "Pgen (MW) | Pmax (MW) | Pmin (MW) | MBASE (MVA) | Qmax (MVAr) | Qmin (MVAr) | Xs (pu) | Cost")
        for i, (_, r) in enumerate(selected_df.iterrows(), start=1):
            bus = int(r["CandidateBus"])
            name_mask = scr_df["BusNumber"] == bus
            name = scr_df.loc[name_mask, "Station Name"].values[0] if (name_mask.any() and "Station Name" in scr_df.columns) else ""
            pgen = float(r["Pgen"]) if "Pgen" in r.index else float(SYNCON_PGEN_MW)
            pmax = float(r["Pmax"]) if "Pmax" in r.index else pgen
            pmin = float(r["Pmin"]) if "Pmin" in r.index else pgen
            details = (f"Pgen={pgen:.1f} | "
                       f"Pmax={pmax:.1f} | "
                       f"Pmin={pmin:.1f} | "
                       f"{r['MBASE_MVA']:.0f} MVA | "
                       f"Qmax={r['QMAX_MVAr']:.0f} | "
                       f"Qmin={r['QMIN_MVAr']:.0f} | "
                       f"Xs={r['Xsource_pu']:.3f} | "
                       f"Cost={r['Cost']:.1f}")
            _row(f"C{i}", str(r["Candidate"]), f"{bus} – {name}", details)
        _row("C-Total", "Total Cost", float(selected_df["Cost"].sum()), "")
    else:
        _row("C1", "No SynCons selected", "No feasible portfolio found within candidate set", "")

    # ── Section D: Target-metric Improvement per Bus After SynCon ───────────
    metric_label = str(target_metric).upper()
    metric_df = scr_df if metric_label == "SCR" else scrif_df
    base_col = f"Base {metric_label}"
    final_col = f"With-SynCon {metric_label}"
    _sep(f"SECTION D: {metric_label} IMPROVEMENT AFTER SYNCON (Base → With SynCon)")
    if not achieved_df.empty:
        _row("", "BusNumber", "Station Name", f"Base {metric_label}  →  With-SynCon {metric_label}  |  Change  |  Status")
        # Get base target metric from the matching metric sheet (numeric rows only)
        base_metric_map = {}
        if base_col in metric_df.columns:
            for _, r in metric_df.iterrows():
                try:
                    bus = int(r["BusNumber"])
                    v = float(r[base_col])
                    if np.isfinite(v):
                        base_metric_map[bus] = v
                except Exception:
                    pass

        for i, (_, r) in enumerate(achieved_df.iterrows(), start=1):
            bus = int(r["BusNumber"])
            new_val = float(r["FinalMetric"]) if np.isfinite(float(r["FinalMetric"])) else np.nan
            base_val = base_metric_map.get(bus, np.nan)
            name_mask = metric_df["BusNumber"] == bus
            name = metric_df.loc[name_mask, "Station Name"].values[0] if (name_mask.any() and "Station Name" in metric_df.columns) else ""
            sat = int(r.get("Satisfied", 0))
            status = "✔ PASS" if sat else "✘ FAIL"
            base_str = f"{base_val:.3f}" if np.isfinite(base_val) else "N/A"
            new_str  = f"{new_val:.3f}" if np.isfinite(new_val) else "N/A"
            if np.isfinite(base_val) and np.isfinite(new_val):
                change = new_val - base_val
                change_str = f"{change:+.3f}"
            else:
                change_str = "N/A"
            _row(f"D{i}", str(bus), name, f"{base_str}  →  {new_str}  |  {change_str}  |  {status}")
    else:
        _row("D1", f"No post-SynCon {metric_label} data available", "", "")

    # ── Section E: Screening Top Candidates ─────────────────────────────────
    _sep(f"SECTION E: TOP SCREENING RESULTS ({metric_label}, by Deficit Reduction)")
    if not screening_df.empty:
        _row("", "Rank", "Candidate  (Bus)", f"{metric_label} Deficit Reduction | FeasibleAlone | Status")
        top_n = min(10, len(screening_df))
        for i, (_, r) in enumerate(screening_df.head(top_n).iterrows(), start=1):
            bus = int(r["CandidateBus"])
            name_mask = scr_df["BusNumber"] == bus
            name = scr_df.loc[name_mask, "Station Name"].values[0] if (name_mask.any() and "Station Name" in scr_df.columns) else ""
            dr = r.get("DeficitReduction", np.nan)
            fa = int(r.get("FeasibleAlone", 0))
            dr_str = f"{dr:.4f}" if np.isfinite(float(dr)) else "N/A"
            _row(f"E{i}", str(i), f"{r['Candidate']}  ({bus} – {name})",
                 f"ΔDeficit={dr_str}  |  FeasibleAlone={'YES' if fa else 'NO'}  |  {r.get('Status','')}")
    else:
        _row("E1", "Screening not run", "", "")

    return pd.DataFrame(rows, columns=["#", "Item", "Value", "Notes"])



def save_final_optimal_case(
    selected_df: pd.DataFrame,
    log_path: str,
    out_case_path: str,
) -> tuple[bool, str]:
    """
    Rebuild the validated final selected SynCon portfolio on top of BASE_TEMP,
    solve it once more, and save the resulting PSSE .sav case.
    """
    if selected_df is None or selected_df.empty:
        return False, "NO SELECTED SYNCON PORTFOLIO"

    with _OsSilencer(_PSSE_SILENT_LOG):
        psspy.case(BASE_TEMP)

    try:
        add_syncon_subset_to_case(selected_df)
    except Exception as e:
        return False, f"ADD FAILED: {e}"

    ok, msg = solve_case_safe_n(log_path, n=3)
    if not ok:
        return False, f"SOLVE FAILED: {msg}"

    try:
        with _OsSilencer(_PSSE_SILENT_LOG):
            psspy.save(out_case_path)
    except Exception as e:
        return False, f"SAVE FAILED: {e}"

    return True, f"SAVED: {out_case_path}"



# ==========================================================
# MAIN
# ==========================================================
def main():
    df_bus, df_con, df_ded = read_inputs(INPUT_XLSX)
    df_syn = read_syncon_inputs(INPUT_XLSX)

    buses = df_bus["BusNumber"].tolist()
    p_mw = dict(zip(df_bus["BusNumber"], df_bus["Installed Capacity"]))
    q_step_by_bus = dict(zip(df_bus["BusNumber"], df_bus["Q_step_MVAr"]))
    deduction_mva_by_bus = build_fault_mva_deduction_map(df_ded, buses)

    dq_table: list[dict] = []

    # ========== BASE ==========
    psspy.case(CASE_PATH)
    base_log = os.path.join(LOG_DIR, "base_case.log")

    with PsseLogCapture(base_log):
        ok, msg = solve_case_safe_n(base_log, n=2)
        if not ok:
            raise RuntimeError(f"Base case did not solve: {msg}")
        with _OsSilencer(_PSSE_SILENT_LOG):
            psspy.save(BASE_TEMP)

        base_scmva_raw = get_fault_mva_iec60909(buses)
        base_scmva = apply_fault_mva_deduction(base_scmva_raw, deduction_mva_by_bus, buses)
        base_scr = compute_scr(base_scmva, p_mw, buses)

        base_IF, base_status, base_dq, base_scrif_fail = compute_if_matrix(buses, q_step_by_bus, base_log, case_name="BASE")
        dq_table.extend(base_dq)

        if base_scrif_fail is not None:
            base_scrif = {b: base_scrif_fail for b in buses}
        else:
            base_scrif = compute_scrif(base_scmva, base_IF, p_mw, buses)

    print(f"Buses: {len(df_bus)}")
    print(f"Contingencies: {len(df_con)}")
    print(f"SynCon candidates: {0 if df_syn is None else len(df_syn)}")

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

        with _OsSilencer(_PSSE_SILENT_LOG):
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

            if ok:
                ok, msg = solve_case_safe_n(log_file, n=3)

            if not ok:
                scr_df[scr_col] = msg
                scrif_df[scrif_col] = msg
                print(f"❌ Case-{k} FAILED: {msg}")
                continue

            c_scmva_raw = get_fault_mva_iec60909(buses)
            c_scmva = apply_fault_mva_deduction(c_scmva_raw, deduction_mva_by_bus, buses)
            c_scr = compute_scr(c_scmva, p_mw, buses)
            scr_df[scr_col] = scr_df["BusNumber"].map(c_scr)

            c_IF, c_status, c_dq, c_scrif_fail = compute_if_matrix(buses, q_step_by_bus, log_file, case_name=f"Case-{k}")
            dq_table.extend(c_dq)

            if c_scrif_fail is not None:
                scrif_df[scrif_col] = c_scrif_fail
            else:
                c_scrif = compute_scrif(c_scmva, c_IF, p_mw, buses)
                scrif_df[scrif_col] = scrif_df["BusNumber"].map(c_scrif)

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
        desc = str(row[desc_col]).strip() if desc_col else ""
        if not desc:
            desc = default_contingency_description(row)

        r1 = {c: np.nan for c in scr_df.columns}
        r1["BusNumber"] = f"Case-{k}"
        if "Station Name" in scr_df.columns:
            r1["Station Name"] = desc
        scr_rows.append(r1)

        r2 = {c: np.nan for c in scrif_df.columns}
        r2["BusNumber"] = f"Case-{k}"
        if "Station Name" in scrif_df.columns:
            r2["Station Name"] = desc
        scrif_rows.append(r2)

    if scr_rows:
        scr_df = pd.concat([scr_df, pd.DataFrame(scr_rows, columns=scr_df.columns)], ignore_index=True)
    if scrif_rows:
        scrif_df = pd.concat([scrif_df, pd.DataFrame(scrif_rows, columns=scrif_df.columns)], ignore_index=True)

    # ========== Optimization ==========
    base_target_metric = base_scr if str(OPT_TARGET_METRIC).strip().upper() == "SCR" else base_scrif
    base_feasible, base_metric_detail = metric_to_detail_rows(base_target_metric, OPT_MIN_THRESHOLD, buses)

    screening_df = pd.DataFrame()
    shortlist_df = pd.DataFrame()
    selected_df = pd.DataFrame()
    achieved_df = pd.DataFrame()
    validation_df = pd.DataFrame()
    trace_df = pd.DataFrame()
    summary_df = pd.DataFrame()
    final_case_info_df = pd.DataFrame()

    if ENABLE_SYNCON_OPTIMIZATION and df_syn is not None and not df_syn.empty:
        if base_feasible and not INCLUDE_ALREADY_FEASIBLE_BASE:
            print("\nBase case already satisfies threshold at all buses. Optimization skipped.")
            validation_df = base_metric_detail.copy()
            summary_df = pd.DataFrame([{
                "OptimizationMode": "SKIPPED (BASE ALREADY FEASIBLE)",
                "TargetMetric": str(OPT_TARGET_METRIC).upper(),
                "Threshold": float(OPT_MIN_THRESHOLD),
                "BaseAlreadyFeasible": 1,
                "OriginalCandidateCount": len(df_syn),
                "SelectedCount": 0,
                "TotalCost": 0.0,
                "ValidationFeasible": 1,
                "GlobalBenefit": float(global_benefit_score(base_target_metric, buses)),
            }])
        else:
            if str(OPTIMIZATION_MODE).strip().upper() == "FAST_HYBRID":
                screen_log = os.path.join(LOG_DIR, "screening.log")
                with PsseLogCapture(screen_log):
                    screening_df = screen_candidates_one_by_one(
                        df_syn=df_syn,
                        monitored_buses=buses,
                        p_mw=p_mw,
                        base_if=base_IF,
                        base_metric=base_target_metric,
                        target_metric=OPT_TARGET_METRIC,
                        threshold=OPT_MIN_THRESHOLD,
                        log_path=screen_log,
                        q_step_by_bus=q_step_by_bus,
                        deduction_mva_by_bus=deduction_mva_by_bus,
                    )

                # Identify buses that are below threshold in the base case
                _, base_det_df = metric_to_detail_rows(base_target_metric, OPT_MIN_THRESHOLD, buses)
                deficient_buses = set(base_det_df.loc[base_det_df["Satisfied"] == 0, "BusNumber"].tolist())

                # Build shortlist: top-N by ranking PLUS any candidate sitting at a deficient bus
                shortlist_names_top = screening_df.head(min(SHORTLIST_SIZE, len(screening_df)))["Candidate"].tolist()
                deficient_candidates = df_syn[df_syn["CandidateBus"].isin(deficient_buses)]["Candidate"].tolist()
                shortlist_names = list(dict.fromkeys(shortlist_names_top + deficient_candidates))  # preserve order, deduplicate

                shortlist_df = df_syn[df_syn["Candidate"].isin(shortlist_names)].copy()
                shortlist_df = shortlist_df.merge(
                    screening_df[["Candidate", "FeasibleAlone", "PassCount", "DeficitReduction", "TotalDeficitAlone"]],
                    on="Candidate",
                    how="left"
                ).sort_values(
                    by=["FeasibleAlone", "DeficitReduction", "PassCount", "Cost"],
                    ascending=[False, False, False, True]
                ).reset_index(drop=True)

                exact_log = os.path.join(LOG_DIR, "exact_shortlist_enumeration.log")
                with PsseLogCapture(exact_log):
                    selected_df, achieved_df, trace_df = exact_enumerate_candidates(
                        df_candidates=shortlist_df,
                        monitored_buses=buses,
                        p_mw=p_mw,
                        base_if=base_IF,
                        target_metric=OPT_TARGET_METRIC,
                        threshold=OPT_MIN_THRESHOLD,
                        log_path=exact_log,
                        max_selected=OPT_MAX_SELECTED,
                        exact_limit=MAX_EXACT_SHORTLIST,
                        q_step_by_bus=q_step_by_bus,
                        deduction_mva_by_bus=deduction_mva_by_bus,
                    )

                mode_label = "FAST HYBRID (actual screening + exact shortlist)"
                orig_count = len(df_syn)
                shortlist_count = len(shortlist_df)

            elif str(OPTIMIZATION_MODE).strip().upper() == "EXACT_FULL":
                exact_full_log = os.path.join(LOG_DIR, "exact_full_enumeration.log")
                with PsseLogCapture(exact_full_log):
                    selected_df, achieved_df, trace_df = exact_enumerate_candidates(
                        df_candidates=df_syn,
                        monitored_buses=buses,
                        p_mw=p_mw,
                        base_if=base_IF,
                        target_metric=OPT_TARGET_METRIC,
                        threshold=OPT_MIN_THRESHOLD,
                        log_path=exact_full_log,
                        max_selected=OPT_MAX_SELECTED,
                        exact_limit=MAX_EXACT_FULL,
                        q_step_by_bus=q_step_by_bus,
                        deduction_mva_by_bus=deduction_mva_by_bus,
                    )
                mode_label = "EXACT FULL ENUMERATION"
                orig_count = len(df_syn)
                shortlist_count = len(df_syn)
            else:
                raise ValueError("OPTIMIZATION_MODE must be FAST_HYBRID or EXACT_FULL")

            if not selected_df.empty:
                val_log = os.path.join(LOG_DIR, "selected_portfolio_validation.log")
                with PsseLogCapture(val_log):
                    feasible, cost, metric, status, det_df = evaluate_subset(
                        subset_df=selected_df,
                        monitored_buses=buses,
                        p_mw=p_mw,
                        base_if=base_IF,
                        target_metric=OPT_TARGET_METRIC,
                        threshold=OPT_MIN_THRESHOLD,
                        log_path=val_log,
                        q_step_by_bus=q_step_by_bus,
                        deduction_mva_by_bus=deduction_mva_by_bus,
                    )
                validation_df = det_df.copy()
                summary_df = pd.DataFrame([{
                    "OptimizationMode": mode_label,
                    "TargetMetric": str(OPT_TARGET_METRIC).upper(),
                    "Threshold": float(OPT_MIN_THRESHOLD),
                    "BaseAlreadyFeasible": int(base_feasible),
                    "OriginalCandidateCount": orig_count,
                    "SearchSetCount": shortlist_count,
                    "SelectedCount": len(selected_df),
                    "TotalCost": float(selected_df["Cost"].sum()),
                    "ValidationStatus": status,
                    "ValidationFeasible": int(feasible),
                    "GlobalBenefit": float(global_benefit_score(metric, buses)) if feasible else np.nan,
                }])
            else:
                summary_df = pd.DataFrame([{
                    "OptimizationMode": mode_label,
                    "TargetMetric": str(OPT_TARGET_METRIC).upper(),
                    "Threshold": float(OPT_MIN_THRESHOLD),
                    "BaseAlreadyFeasible": int(base_feasible),
                    "OriginalCandidateCount": orig_count,
                    "SearchSetCount": shortlist_count,
                    "SelectedCount": 0,
                    "TotalCost": np.nan,
                    "ValidationStatus": "NO FEASIBLE PORTFOLIO FOUND",
                    "ValidationFeasible": 0,
                    "GlobalBenefit": np.nan,
                }])


    # ========== Save validated final optimal .sav case ==========
    if (
        SAVE_FINAL_OPTIMAL_CASE
        and not selected_df.empty
        and not summary_df.empty
        and int(summary_df.iloc[0].get("ValidationFeasible", 0)) == 1
    ):
        final_case_log = os.path.join(LOG_DIR, "final_optimal_case_save.log")
        with PsseLogCapture(final_case_log):
            saved_ok, saved_msg = save_final_optimal_case(
                selected_df=selected_df,
                log_path=final_case_log,
                out_case_path=FINAL_OPTIMAL_CASE_PATH,
            )
        final_case_info_df = pd.DataFrame([{
            "SaveRequested": int(SAVE_FINAL_OPTIMAL_CASE),
            "FinalCasePath": FINAL_OPTIMAL_CASE_PATH,
            "Status": saved_msg,
            "SavedOK": int(saved_ok),
            "SelectedCount": int(len(selected_df)),
        }])
    elif SAVE_FINAL_OPTIMAL_CASE:
        final_case_info_df = pd.DataFrame([{
            "SaveRequested": int(SAVE_FINAL_OPTIMAL_CASE),
            "FinalCasePath": FINAL_OPTIMAL_CASE_PATH,
            "Status": "SKIPPED (NO VALIDATED FEASIBLE PORTFOLIO)",
            "SavedOK": 0,
            "SelectedCount": int(len(selected_df)),
        }])

    # ========== ΔQ Used Sheet ==========
    dq_df = pd.DataFrame(dq_table)
    if not dq_df.empty:
        dq_df["CaseSort"] = dq_df["Case"].apply(case_sort_key)
        dq_df = dq_df.sort_values(["CaseSort", "BusNumber"]).drop(columns=["CaseSort"])

    # ========== Clean Results Summary ==========
    summary_report_df = build_results_summary(
        scr_df=scr_df,
        scrif_df=scrif_df,
        selected_df=selected_df,
        achieved_df=achieved_df,
        validation_df=validation_df,
        summary_df=summary_df,
        screening_df=screening_df,
        base_metric_detail=base_metric_detail,
        threshold=OPT_MIN_THRESHOLD,
        target_metric=OPT_TARGET_METRIC,
    )

    # ========== Save ==========
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        # ---- Put Results_Summary first so it opens as the active sheet ----
        if not summary_report_df.empty:
            summary_report_df.to_excel(writer, sheet_name="Results_Summary", index=False)
        scr_df.to_excel(writer, sheet_name="SCR", index=False)
        scrif_df.to_excel(writer, sheet_name="SCRIF", index=False)
        if not dq_df.empty:
            dq_df.to_excel(writer, sheet_name="ΔQ_Used", index=False)
        if not base_metric_detail.empty:
            base_metric_detail.to_excel(writer, sheet_name="Base_TargetMetric_Check", index=False)
        if df_syn is not None and not df_syn.empty:
            df_syn.to_excel(writer, sheet_name="SynCon_Input_Normalized", index=False)
        if not screening_df.empty:
            screening_df.to_excel(writer, sheet_name="SynCon_Screening", index=False)
        if not shortlist_df.empty:
            shortlist_df.to_excel(writer, sheet_name="SynCon_Shortlist", index=False)
        if not selected_df.empty:
            selected_df.to_excel(writer, sheet_name="SynCon_Selected", index=False)
        if not achieved_df.empty:
            achieved_df.to_excel(writer, sheet_name="SynCon_Achieved", index=False)
        if not validation_df.empty:
            validation_df.to_excel(writer, sheet_name="SynCon_Validation", index=False)
        if not trace_df.empty:
            trace_df.to_excel(writer, sheet_name="SynCon_Trace", index=False)
        if not summary_df.empty:
            summary_df.to_excel(writer, sheet_name="SynCon_Optimization", index=False)
        if not final_case_info_df.empty:
            final_case_info_df.to_excel(writer, sheet_name="SynCon_FinalCase", index=False)

    print(f"\nSaved: {OUTPUT_XLSX}")
    if not final_case_info_df.empty:
        print(f"Final optimal case: {final_case_info_df.iloc[0]['Status']}")
    print(f"PSSE logs saved in: {LOG_DIR}")


if __name__ == "__main__":
    main()
