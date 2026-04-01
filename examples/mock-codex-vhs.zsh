#!/bin/zsh
set -euo pipefail

last_message=""
while (( $# > 0 )); do
  case "$1" in
    --output-last-message)
      last_message="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

label="${${last_message:t}%.last.md}"
root="${last_message:h:h}"
mkdir -p "$root"

case "$label" in
  intake)
    cat > "$root/brief.md" <<'EOF'
# Brief

Mock intake completed.
EOF
    cat > "$last_message" <<'EOF'
Mock intake complete.
EOF
    ;;
  solver-*)
    mkdir -p "$root/solutions/$label"
    cat > "$root/solutions/$label/RESULT.md" <<EOF
# Result

Mock solution from $label.
EOF
    cat > "$last_message" <<EOF
Mock $label complete.
EOF
    ;;
  review)
    mkdir -p "$root/review"
    cat > "$root/review/report.md" <<'EOF'
# Review Report

Mock review complete.
EOF
    cat > "$root/review/scorecard.json" <<'JSON'
{
  "winner": "solver-a",
  "backup": "solver-b",
  "risks": []
}
JSON
    cat > "$root/review/user-summary.md" <<'EOF'
# User Summary

Mock review summary.
EOF
    cat > "$last_message" <<'EOF'
Mock review complete.
EOF
    ;;
  execution)
    mkdir -p "$root/execution"
    cat > "$root/execution/report.md" <<'EOF'
# Execution Report

Mock execution complete.
EOF
    cat > "$last_message" <<'EOF'
Mock execution complete.
EOF
    ;;
  verification)
    mkdir -p "$root/verification"
    cat > "$root/verification/findings.md" <<'EOF'
# Findings

Mock verification complete.
EOF
    cat > "$root/verification/user-summary.md" <<'EOF'
# Verification Summary

Mock verification summary.
EOF
    cat > "$root/verification/improvement-request.md" <<'EOF'
# Improvement Request

No follow-up changes required.
EOF
    cat > "$root/verification/augmented-task.md" <<'EOF'
# Augmented Task

No follow-up run required.
EOF
    cat > "$root/verification/goal-status.json" <<'JSON'
{
  "goal_complete": true,
  "goal_verdict": "complete",
  "rerun_recommended": false,
  "recommended_next_action": "none"
}
JSON
    cat > "$last_message" <<'EOF'
Mock verification complete.
EOF
    ;;
  *)
    cat > "$last_message" <<EOF
Mock $label complete.
EOF
    ;;
esac

print "mock $label complete"
