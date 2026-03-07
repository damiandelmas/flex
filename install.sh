#!/bin/bash
# Flex installer — https://getflex.dev
# Usage: curl -sSL https://getflex.dev/install.sh | bash
#
# Options (via env vars or flags):
#   FLEX_VERSION=0.5.0    pin a specific version
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
                echo "  FLEX_VERSION    pin a specific version (e.g. 0.5.0)"
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
                # Skip pythons inside a venv/conda env — we need a system python
                local in_venv
                in_venv=$("$cmd" -c "import sys; print(int(sys.prefix != sys.base_prefix or hasattr(sys, 'real_prefix')))" 2>/dev/null) || continue
                [ "$in_venv" = "1" ] && continue

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

    PYTHON=$(find_python) || fail "Python ${MIN_PYTHON}+ not found (venv/conda pythons are skipped).\n  Deactivate your environment or install a system python:\n  Mac:    brew install python@3.12\n  Ubuntu: sudo apt install python3.12 python3.12-venv\n  Any:    https://python.org/downloads/"

    PYTHON_VER=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    ok "python" "${PYTHON_VER} ($(command -v "$PYTHON"))"

    # ── Check dependencies ───────────────────────────────────────
    need_cmd git "Mac: xcode-select --install  Linux: sudo apt install git"
    need_cmd jq  "Mac: brew install jq  Linux: sudo apt install jq"

    # ── Clean prior package manager installs ─────────────────────
    "$PYTHON" -m pip uninstall -y getflex &>/dev/null || true
    if command -v pipx &>/dev/null; then
        pipx uninstall getflex &>/dev/null || true
    fi
    rm -f "${BIN_DIR}/flx" 2>/dev/null || true
    if [ -L "${BIN_DIR}/flex" ]; then
        _target=$(readlink "${BIN_DIR}/flex" 2>/dev/null || true)
        case "$_target" in
            "${VENV_DIR}/"*) ;;
            *) rm -f "${BIN_DIR}/flex" ;;
        esac
    fi

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

    # ── Upgrade guard: remove old PyPI install if present ────────
    local _old_ver
    _old_ver=$("${VENV_DIR}/bin/python" -c "from importlib.metadata import version; print(version('getflex'))" 2>/dev/null || echo "")
    if [ -n "$_old_ver" ]; then
        # Compare major.minor — anything 0.x where x < 5 was PyPI-distributed
        local _old_major _old_minor
        _old_major=$(echo "$_old_ver" | cut -d. -f1)
        _old_minor=$(echo "$_old_ver" | cut -d. -f2)
        if [ "$_old_major" -eq 0 ] 2>/dev/null && [ "$_old_minor" -lt 5 ] 2>/dev/null; then
            _spin_start "upgrade" "removing old PyPI install (v${_old_ver})"
            "${VENV_DIR}/bin/pip" uninstall -y getflex &>/dev/null || true
            _spin_stop
            ok "upgrade" "v${_old_ver} → clean"
        fi
    fi

    # ── Install getflex ──────────────────────────────────────────
    FLEX_VERSION="${FLEX_VERSION:-0.5.0}"

    # Skip if already on target version (unless --reinstall)
    local _cur_ver
    _cur_ver=$("${VENV_DIR}/bin/python" -c "from importlib.metadata import version; print(version('getflex'))" 2>/dev/null || echo "")
    if [ "$_cur_ver" = "$FLEX_VERSION" ] && [ "$DO_REINSTALL" = false ]; then
        ok "install" "getflex ${FLEX_VERSION} (already installed)"
    else
        "${VENV_DIR}/bin/pip" cache remove getflex &>/dev/null || true
        rm -rf ~/.cache/pip/wheels/**/getflex-* 2>/dev/null || true

        local _whl_url="https://github.com/damiandelmas/flex/releases/download/v${FLEX_VERSION}/getflex-${FLEX_VERSION}-py3-none-any.whl"
        local _whl_tmp="/tmp/getflex-${FLEX_VERSION}-py3-none-any.whl"

        _spin_start "install" "downloading getflex ${FLEX_VERSION}"
        if ! curl -sSL -o "$_whl_tmp" "$_whl_url" 2>/dev/null; then
            # Fallback to mirror
            local _mirror="https://getflex.dev/releases/getflex-${FLEX_VERSION}-py3-none-any.whl"
            if ! curl -sSL -o "$_whl_tmp" "$_mirror" 2>/dev/null; then
                _spin_stop
                fail "Failed to download getflex ${FLEX_VERSION}.\n  URL: ${_whl_url}"
            fi
        fi
        _spin_stop

        # Validate download is actually a wheel (not an HTML error page)
        if ! file "$_whl_tmp" 2>/dev/null | grep -q "Zip archive"; then
            rm -f "$_whl_tmp"
            fail "Download failed — got an invalid file instead of a wheel.\n  Your network may block GitHub releases.\n  Try: curl -L -o /tmp/getflex.whl ${_whl_url}\n       ${VENV_DIR}/bin/pip install /tmp/getflex.whl"
        fi

        _spin_start "install" "pip install getflex ${FLEX_VERSION}"
        local _pip_log="/tmp/getflex-pip-install.log"
        if ! "${VENV_DIR}/bin/pip" install --upgrade --quiet --retries 3 "$_whl_tmp" > "$_pip_log" 2>&1; then
            _spin_stop
            echo ""
            echo "  pip output:"
            sed 's/^/    /' "$_pip_log"
            echo ""
            fail "pip install failed.\n  Wheel saved at: ${_whl_tmp}\n  Try manually: ${VENV_DIR}/bin/pip install ${_whl_tmp}"
        fi
        rm -f "$_whl_tmp" "$_pip_log"

        # Verify import works (deep check — exercise all C extensions)
        if ! "${VENV_DIR}/bin/python" -c "import flex" &>/dev/null; then
            _spin_stop
            fail "Installation broken — import flex failed.\n  Try: ${VENV_DIR}/bin/pip install --force-reinstall getflex"
        fi
        if ! "${VENV_DIR}/bin/python" -c "import numpy; import onnxruntime; import sklearn; import tokenizers" 2>/dev/null; then
            _spin_stop
            fail "Native extensions broken (numpy/onnxruntime/sklearn/tokenizers).\n  This often happens on HPC clusters with non-standard system libraries.\n  Try: ${VENV_DIR}/bin/pip install --force-reinstall numpy>=2.0 onnxruntime scikit-learn tokenizers"
        fi
        _spin_stop

        VER=$("${VENV_DIR}/bin/python" -c "from importlib.metadata import version; print(version('getflex'))")
        ok "install" "getflex ${VER}"
    fi

    # ── Symlink to PATH ──────────────────────────────────────────
    mkdir -p "$BIN_DIR"

    # Remove stale symlink if it exists
    if [ -L "${BIN_DIR}/flex" ]; then
        rm "${BIN_DIR}/flex"
    fi

    # Only symlink if there's no non-symlink flex binary (e.g. GNU flex)
    if [ ! -e "${BIN_DIR}/flex" ]; then
        ln -s "${VENV_DIR}/bin/flex" "${BIN_DIR}/flex"
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
