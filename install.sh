#!/bin/bash
# Flex installer — https://getflex.dev
# Usage: curl -sSL https://getflex.dev/install.sh | bash
#
# Options (via env vars or flags):
#   FLEX_VERSION=0.2.1    pin a specific version
#   --uninstall           remove flex completely
#   --reinstall           wipe venv and reinstall from scratch
#   --no-init             install only, skip flex init
#   --help                show usage

main() {
    set -eo pipefail

    FLEX_HOME="${HOME}/.flex"
    VENV_DIR="${FLEX_HOME}/venv"
    BIN_DIR="${HOME}/.local/bin"
    MIN_PYTHON="3.12"

    # ── Color (respect NO_COLOR / dumb terminals) ────────────────
    if [ -n "${NO_COLOR:-}" ] || [ "${TERM:-}" = "dumb" ] || ! [ -t 1 ]; then
        RED='' GREEN='' YELLOW='' CYAN='' DIM='' BOLD='' RESET=''
    else
        RED='\033[0;31m' GREEN='\033[0;32m' YELLOW='\033[0;33m' CYAN='\033[0;36m' DIM='\033[0;90m'
        BOLD='\033[1m' RESET='\033[0m'
    fi

    info()  { echo -e "${DIM}$1${RESET}"; }
    ok()    { printf "  %-10s${GREEN}%s${RESET}\n" "$1" "$2"; }
    warn()  { printf "${DIM}  %-10s${YELLOW}%s${RESET}\n" "$1" "$2"; }
    fail()  { echo -e "${RED}$1${RESET}" >&2; exit 1; }

    _spin_pid=""
    _spin() {
        local label="$1" msg="$2"
        while true; do
            for c in '⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏'; do
                printf "\r  %-10s%s %s" "$label" "$c" "$msg"
                sleep 0.1
            done
        done
    }
    _spin_start() { _spin "$1" "$2" & _spin_pid=$!; }
    _spin_stop()  { kill "$_spin_pid" 2>/dev/null || true; wait "$_spin_pid" 2>/dev/null || true; printf "\r\033[K"; }

    need_cmd() {
        command -v "$1" &>/dev/null || fail "$1 is required but not found.\n  $2"
    }

    # ── Parse flags ──────────────────────────────────────────────
    DO_UNINSTALL=false
    DO_REINSTALL=false
    DO_INIT=true

    for arg in "$@"; do
        case "$arg" in
            --uninstall)  DO_UNINSTALL=true ;;
            --reinstall)  DO_REINSTALL=true ;;
            --no-init)    DO_INIT=false ;;
            --help|-h)
                echo "Usage: curl -sSL https://getflex.dev/install.sh | bash"
                echo ""
                echo "Options:"
                echo "  --uninstall     remove flex (venv + symlink)"
                echo "  --reinstall     wipe venv and reinstall from scratch"
                echo "  --no-init       install only, skip flex init"
                echo ""
                echo "Environment:"
                echo "  FLEX_VERSION    pin a specific version (e.g. 0.2.1)"
                exit 0
                ;;
            *)
                fail "Unknown option: $arg\n  Run with --help for usage."
                ;;
        esac
    done

    # ── Uninstall ────────────────────────────────────────────────
    if [ "$DO_UNINSTALL" = true ]; then
        echo ""
        echo -e "${CYAN}flex${RESET} uninstaller"
        echo ""
        if [ -L "${BIN_DIR}/flex" ]; then
            rm "${BIN_DIR}/flex"
            ok "removed" "${BIN_DIR}/flex"
        fi
        if [ -d "$VENV_DIR" ]; then
            rm -rf "$VENV_DIR"
            ok "removed" "${VENV_DIR}"
        fi
        echo ""
        info "  Data at ~/.flex/ was NOT removed (cells, registry, models)."
        info "  To remove everything: rm -rf ~/.flex"
        echo ""
        exit 0
    fi

    # ── Banner ───────────────────────────────────────────────────
    echo ""
    echo -e "${CYAN}flex${RESET} installer"
    echo ""

    # ── Find Python 3.12+ ────────────────────────────────────────
    find_python() {
        for cmd in python3.14 python3.13 python3.12 python3; do
            if command -v "$cmd" &>/dev/null; then
                local ver
                ver=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null) || continue
                local major minor
                major=$(echo "$ver" | cut -d. -f1)
                minor=$(echo "$ver" | cut -d. -f2)
                if [ "$major" -ge 3 ] && [ "$minor" -ge 12 ]; then
                    echo "$cmd"
                    return 0
                fi
            fi
        done
        return 1
    }

    PYTHON=$(find_python) || fail "Python ${MIN_PYTHON}+ not found.\n  Mac:    brew install python@3.12\n  Ubuntu: sudo apt install python3.12 python3.12-venv\n  Any:    https://python.org/downloads/"

    PYTHON_VER=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    ok "python" "${PYTHON_VER} ($(command -v "$PYTHON"))"

    # ── Check dependencies ───────────────────────────────────────
    need_cmd git "Mac: xcode-select --install  Linux: sudo apt install git"
    need_cmd jq  "Mac: brew install jq  Linux: sudo apt install jq"

    # ── Check venv module ────────────────────────────────────────
    if ! "$PYTHON" -m venv --help &>/dev/null 2>&1; then
        fail "Python venv module not available.\n  Ubuntu/Debian: sudo apt install python${PYTHON_VER}-venv"
    fi

    # ── Create venv ──────────────────────────────────────────────
    if [ "$DO_REINSTALL" = true ] && [ -d "$VENV_DIR" ]; then
        _spin_start "venv" "removing old"
        rm -rf "$VENV_DIR"
        _spin_stop
        ok "venv" "removed"
    fi

    if [ -d "$VENV_DIR" ]; then
        ok "venv" "exists"
    else
        _spin_start "venv" "creating"
        mkdir -p "$FLEX_HOME"
        if ! "$PYTHON" -m venv "$VENV_DIR"; then
            _spin_stop
            # Clean up partial venv on failure
            rm -rf "$VENV_DIR" 2>/dev/null || true
            fail "Failed to create venv. Check Python installation."
        fi
        _spin_stop
        ok "venv" "ok"
    fi

    # ── Install getflex ──────────────────────────────────────────
    local _pkg="getflex"
    if [ -n "${FLEX_VERSION:-}" ]; then
        _pkg="getflex==${FLEX_VERSION}"
        ok "install" "pinned to ${FLEX_VERSION}"
    fi

    _spin_start "install" "pip install ${_pkg}"
    if ! "${VENV_DIR}/bin/pip" install --upgrade --quiet --retries 3 "$_pkg" 2>&1; then
        _spin_stop
        fail "pip install failed.\n  Try manually: ${VENV_DIR}/bin/pip install ${_pkg}"
    fi

    # Verify import works
    if ! "${VENV_DIR}/bin/python" -c "import flex" &>/dev/null; then
        _spin_stop
        fail "Installation broken — import flex failed.\n  Try: ${VENV_DIR}/bin/pip install --force-reinstall getflex"
    fi
    _spin_stop

    VER=$("${VENV_DIR}/bin/python" -c "from importlib.metadata import version; print(version('getflex'))")
    ok "install" "getflex ${VER}"

    # ── Symlink to PATH ──────────────────────────────────────────
    mkdir -p "$BIN_DIR"

    # Remove stale symlink if it exists
    if [ -L "${BIN_DIR}/flex" ]; then
        rm "${BIN_DIR}/flex"
    fi

    # Only symlink if there's no non-symlink flex binary (e.g. GNU flex)
    if [ ! -e "${BIN_DIR}/flex" ]; then
        ln -s "${VENV_DIR}/bin/flex" "${BIN_DIR}/flex"
        ok "link" "${BIN_DIR}/flex"
    else
        warn "link" "skipped (${BIN_DIR}/flex exists — may be GNU flex)"
        info "            use: ${VENV_DIR}/bin/flex"
    fi

    # ── Ensure PATH ──────────────────────────────────────────────
    if ! echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
        local _profile
        case "${SHELL:-/bin/bash}" in
            */zsh)  _profile="${HOME}/.zshrc" ;;
            */bash) _profile="${HOME}/.bashrc" ;;
            */fish) _profile="${HOME}/.config/fish/config.fish" ;;
            *)      _profile="${HOME}/.profile" ;;
        esac

        if [ -n "$_profile" ]; then
            case "${SHELL:-}" in
                */fish)
                    echo "fish_add_path ${BIN_DIR}" >> "$_profile"
                    ;;
                *)
                    echo "export PATH=\"${BIN_DIR}:\$PATH\"" >> "$_profile"
                    ;;
            esac
            export PATH="${BIN_DIR}:$PATH"
        fi
    fi
    ok "path" "ok"

    # ── Run flex init ────────────────────────────────────────────
    if [ "$DO_INIT" = true ]; then
        echo ""
        info "  Running flex init..."
        echo ""

        # Reconnect /dev/tty so flex init can prompt interactively
        # even when this script is piped via curl | bash
        if [ ! -t 0 ] && [ -e /dev/tty ]; then
            "${VENV_DIR}/bin/flex" init < /dev/tty
        else
            "${VENV_DIR}/bin/flex" init
        fi
    fi

    echo ""
    echo -e "  ${GREEN}Done.${RESET}"
    echo ""
}

main "$@"
