import subprocess
import sys
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
VERIFIER_DIR = ROOT / "vm3/verifier"
REPORT_DIR = VERIFIER_DIR / "reports"

REPORT_DIR.mkdir(parents=True, exist_ok=True)

scripts = [
    ("Input and ground-truth validation", "run_verification.py"),
    ("R1 - High-value transactions", "verify_r1.py"),
    ("R2 - Transaction velocity", "verify_r2.py"),
    ("R3 - High cumulative amount", "verify_r3.py"),
    ("R4 - Repeated destination pattern", "verify_r4.py"),
    ("R5 - Final decision policy", "verify_r5.py"),
]

results = []
full_report = []

print("\n==============================================")
print(" DISTRIBUTED FRAUD MONITORING - MASTER CHECK")
print("==============================================")

for name, script in scripts:
    print(f"\n\n========== {name} ==========")

    result = subprocess.run(
        [sys.executable, str(VERIFIER_DIR / script)],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    output = result.stdout + result.stderr
    print(output)

    full_report.append(
        f"\n\n========== {name} ==========\n{output}"
    )

    if result.returncode != 0 or "FAILED" in output:
        status = "FAIL"
    elif script == "run_verification.py":
        status = (
            "PASS"
            if "VERIFICATION FINISHED" in output
            else "FAIL"
        )
    else:
        status = "PASS" if "PASSED" in output else "FAIL"

    results.append((name, status))

print("\n\n==============================================")
print(" FINAL MASTER VERIFICATION SUMMARY")
print("==============================================")

for name, status in results:
    print(f"{status:6} | {name}")

passed = sum(status == "PASS" for _, status in results)
failed = len(results) - passed

print("----------------------------------------------")
print(f"Passed: {passed}/{len(results)}")
print(f"Failed: {failed}/{len(results)}")

if failed == 0:
    print("\nOVERALL RESULT: ALL CHECKS PASSED")
else:
    print("\nOVERALL RESULT: VERIFICATION FAILED")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
report_path = REPORT_DIR / f"master_report_{timestamp}.txt"

report_path.write_text(
    "\n".join(full_report)
    + "\n\n"
    + "\n".join(f"{status}: {name}" for name, status in results),
    encoding="utf-8",
)

print(f"\nDetailed report saved to: {report_path}")

if failed:
    sys.exit(1)
