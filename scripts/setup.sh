#!/usr/bin/env bash
# Instala y valida el entorno del proyecto según pyproject.toml (requires-python).
# Uso:
#   ./scripts/setup.sh
#   ./scripts/setup.sh --install-system-deps          # Ubuntu/Debian: python3-venv (+ opciones)
#   ./scripts/setup.sh --install-system-deps --with-java
#   PYTHON=/usr/bin/python3.12 ./scripts/setup.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Permite PYTHON o PYTHON3 para elegir intérprete (debe cumplir requires-python)
PYTHON3="${PYTHON3:-${PYTHON:-python3}}"
INSTALL_SYS=0
WITH_JAVA=0

for arg in "$@"; do
  case "$arg" in
    --install-system-deps) INSTALL_SYS=1 ;;
    --with-java) WITH_JAVA=1 ;;
    -h|--help)
      cat <<'EOF'
Uso: ./scripts/setup.sh [opciones]

  Lee requires-python de pyproject.toml, comprueba la versión de Python,
  crea ./venv e instala requirements.txt.

Opciones:
  --install-system-deps   Ubuntu/Debian: sudo apt install python3-venv (si hace falta)
  --with-java             Junto con --install-system-deps: instala OpenJDK 17 (PySpark)

Variables:
  PYTHON o PYTHON3        Intérprete a usar (por defecto: python3)

Ejemplos:
  ./scripts/setup.sh
  ./scripts/setup.sh --install-system-deps
  ./scripts/setup.sh --install-system-deps --with-java
  PYTHON=/usr/bin/python3.12 ./scripts/setup.sh
EOF
      exit 0
      ;;
  esac
done

die() { echo "ERROR: $*" >&2; exit 1; }

# apt-get update puede fallar con código 100 si un .list de terceros está roto (ej. Ookla/packagecloud).
# Eso no impide instalar paquetes de los repositorios oficiales de Ubuntu.
apt_update_best_effort() {
  if sudo apt-get update -qq 2>/dev/null; then
    return 0
  fi
  echo "" >&2
  echo "AVISO: apt-get update falló (a menudo un repo extra en /etc/apt/sources.list.d/)." >&2
  echo "  Ejemplo conocido: packagecloud.io/ookla/speedtest-cli — desactívalo o bórralo si no lo usas." >&2
  echo "  Se intenta igualmente: apt-get install ..." >&2
  echo "" >&2
  return 0
}

# --- 1) Intérprete presente ---
command -v "$PYTHON3" >/dev/null || die "No se encontró '$PYTHON3'. Instala Python 3 o define PYTHON=/ruta/al/python3"

# --- 2) Leer requires-python desde pyproject.toml y validar versión ---
read -r MIN_MAJOR MIN_MINOR <<<"$("$PYTHON3" - <<'PY'
import pathlib, re, sys

def parse_min(spec: str) -> tuple[int, int]:
    spec = spec.strip()
    m = re.search(r">=\s*(\d+)\.(\d+)", spec)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r">=\s*(\d+)\b", spec)
    if m:
        return int(m.group(1)), 0
    return 3, 9

text = pathlib.Path("pyproject.toml").read_text(encoding="utf-8")
m = re.search(r'requires-python\s*=\s*"([^"]+)"', text)
spec = m.group(1) if m else ">=3.9"
maj, mino = parse_min(spec)
print(maj, mino)
PY
)"

"$PYTHON3" - "$MIN_MAJOR" "$MIN_MINOR" <<'PY' || exit 1
import sys
maj, mino = int(sys.argv[1]), int(sys.argv[2])
if sys.version_info < (maj, mino):
    print(
        f"ERROR: Este proyecto requiere Python >= {maj}.{mino} (según pyproject.toml). "
        f"Tienes {sys.version_info.major}.{sys.version_info.minor}.",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"OK: Python {sys.version_info.major}.{sys.version_info.minor} cumple >= {maj}.{mino}")
PY

# --- 3) Comprobar si se puede crear venv (ensurepip) ---
test_venv() {
  local t
  t="$(mktemp -d)"
  if "$PYTHON3" -m venv "$t" 2>/dev/null; then
    rm -rf "$t"
    return 0
  fi
  rm -rf "$t"
  return 1
}

if ! test_venv; then
  echo "No se puede crear un venv con '$PYTHON3' (falta soporte venv/ensurepip en el sistema)." >&2
  if [ "$INSTALL_SYS" -eq 1 ]; then
    if [ -f /etc/os-release ]; then . /etc/os-release; fi
    case "${ID:-}" in
      ubuntu|debian)
        echo "Instalando paquetes del sistema (sudo)..." >&2
        apt_update_best_effort
        sudo apt-get install -y python3-venv
        ;;
      *)
        die "Sistema no soportado automáticamente. Instala el paquete equivalente a 'python3-venv' para tu distro."
        ;;
    esac
    if ! test_venv; then
      die "Tras instalar python3-venv, sigue fallando la creación del venv."
    fi
  elif [ -t 0 ] && [ -f /etc/os-release ]; then
    # Terminal interactiva: ofrecer instalar sin tener que recordar flags
    # shellcheck source=/dev/null
    . /etc/os-release
    case "${ID:-}" in
      ubuntu|debian)
        echo "" >&2
        read -r -p "¿Instalar python3-venv con sudo ahora? [s/N] " _ans || true
        _ans="${_ans:-}"
        _ans="$(echo "$_ans" | tr '[:upper:]' '[:lower:]' | xargs)"
        if [[ "$_ans" == s || "$_ans" == si || "$_ans" == y || "$_ans" == yes ]]; then
          apt_update_best_effort
          sudo apt-get install -y python3-venv
          if ! test_venv; then
            die "Tras instalar python3-venv, sigue fallando la creación del venv."
          fi
        else
          echo "" >&2
          echo "  Instala a mano: sudo apt install -y python3-venv" >&2
          echo "  O sin preguntas: ./scripts/setup.sh --install-system-deps" >&2
          echo "" >&2
          exit 1
        fi
        ;;
      *)
        echo "" >&2
        echo "  Instala el paquete 'python3-venv' (o equivalente) para tu distro." >&2
        echo "" >&2
        exit 1
        ;;
    esac
  else
    echo "" >&2
    echo "  Ubuntu/Debian: sudo apt install -y python3-venv" >&2
    echo "  Sin preguntas:   ./scripts/setup.sh --install-system-deps" >&2
    echo "" >&2
    exit 1
  fi
fi

# --- 4) JDK para PySpark (opcional pero recomendado) ---
if [ "$WITH_JAVA" -eq 1 ]; then
  if [ -f /etc/os-release ]; then . /etc/os-release; fi
  case "${ID:-}" in
    ubuntu|debian)
      echo "Instalando OpenJDK 17 (sudo)..." >&2
      apt_update_best_effort
      sudo apt-get install -y openjdk-17-jdk-headless
      ;;
    *)
      echo "AVISO: --with-java solo automatiza apt en Debian/Ubuntu. Instala JDK 17 manualmente." >&2
      ;;
  esac
fi

if ! command -v java >/dev/null 2>&1; then
  echo "AVISO: 'java' no está en PATH. PySpark lo necesita; instala JDK 17 o usa Docker." >&2
else
  echo "OK: java encontrado: $(command -v java)"
fi

# --- 5) Crear venv del proyecto e instalar dependencias Python ---
echo "Creando venv en $ROOT/venv ..."
rm -rf venv
"$PYTHON3" -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt
if [ -d .git ]; then
  ./venv/bin/pre-commit install 2>/dev/null || true
else
  echo "AVISO: Sin carpeta .git — pre-commit no se instaló. Opcional: git init && ./venv/bin/pre-commit install" >&2
fi

echo ""
echo "Listo. Activa el entorno:  source venv/bin/activate"
echo "O usa make:                make test   # (Makefile usa venv/bin/python si existe)"
echo ""
