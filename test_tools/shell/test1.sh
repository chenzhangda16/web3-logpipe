##!/usr/bin/env bash
#f() {
#  local __out_x="$1"; shift
#  local __out_pid="$1"; shift
#
#  sleep 1000 &
#  local __pid=$!
#
#  local __x="hello-from-f"
#
#  printf -v "$__out_x"  '%s' "$__x"
#  printf -v "$__out_pid" '%s' "$__pid"
#}
#
#main() {
#  local x pid
#  f x pid
#  echo "x=$x pid=$pid"
#}
#main

#!/usr/bin/env bash
f() {
  local __out_x="$1"; shift
  local __out_pid="$1"; shift

  sleep 1000 &
  local pid=$!

  local x="hello-from-f"

  printf -v "$__out_x"  '%s' "$x"
  printf -v "$__out_pid" '%s' "$pid"
}

main() {
  local x pid
  f x pid
  echo "x=$x pid=$pid"
}
main

