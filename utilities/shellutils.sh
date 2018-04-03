confirmed () {
  while true
  do
    read RESPONSE
    case $RESPONSE in
      y)
        return 0
        ;;
      n)
        return 1
        ;;
      *)
        echo "Please answer in y/n."
        ;;
    esac
  done
}

require () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Fatal] Failed: $@"
  exit 1
}

warnifnot () {
  "$@" >/dev/null 2>&1 && return 0
  echo
  echo "[Warning] Failed: $@"
  echo
  WARNING=true
}
