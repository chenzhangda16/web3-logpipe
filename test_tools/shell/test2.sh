#!/usr/bin/env bash
f() {
  echo "inside f"
#  sleep 1000 > >(cat >/dev/null) 2>&1 &
#  (sleep 1000 > >(cat >/dev/null) 2>&1) &
  sleep 1000 &
  echo "$!"
}

echo "before"
x="$(f)"
echo "after: $x"
#sleep 1000 > >(cat >/dev/null) 2>&1 &
#pkill -f "sleep 1000"
