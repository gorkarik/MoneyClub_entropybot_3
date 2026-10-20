#!/usr/bin/env bash
# MoneyClub версия 3 — установка на сервер Ubuntu одной командой:
#
#   curl -fsSL https://raw.githubusercontent.com/gorkarik/MoneyClub_entropybot_3/main/install.sh | bash
#
# или из скачанной папки:  bash MoneyClub_entropybot_3/install.sh
#
# Код ставится в папку ~/money-club-3. Меню открывается само при входе на
# сервер по SSH, а после выхода из меню его открывает команда moneyclub.
# Повторный запуск обновляет код, а ключи (.env), настройки (config.yaml,
# tickers/) и журналы (logs/) остаются как были.
# Если на сервере уже есть MoneyClub версии 2 с командой moneyclub, её
# команда переименовывается в moneyclub2, сам бот и его папка не трогаются.
#
# Всё обёрнуто в функцию main и вызывается в самом конце — если файл
# оборвётся на середине, недописанный скрипт просто не выполнится.

main() {
  set -euo pipefail

  # адрес репозитория (если назовёте его иначе — поменяйте здесь)
  local REPO_URL="${MONEYCLUB_REPO:-https://github.com/gorkarik/MoneyClub_entropybot_3.git}"
  local SRC=""
  if [ -n "${BASH_SOURCE[0]:-}" ] && [ -f "${BASH_SOURCE[0]}" ]; then
    SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    { [ -f "$SRC/club.py" ] && [ -d "$SRC/entropy_arb" ]; } || SRC=""
  fi
  local APP_DIR="${MONEYCLUB_DIR:-$HOME/money-club-3}"
  local CMD="${MONEYCLUB_CMD:-moneyclub}"

  say()  { printf '\n  \033[1m%s\033[0m\n' "$*"; }
  ok()   { printf '  ✔ %s\n' "$*"; }
  fail() { printf '\n  ✘ %s\n\n' "$*" >&2; exit 1; }

  printf '\n  \033[1;92mM O N E Y   C L U B\033[0m  ·  версия 3  —  установка\n'

  # ---- права: root работает напрямую, обычный пользователь — через sudo
  local SUDO=""
  if [ "$(id -u)" -ne 0 ]; then
    command -v sudo >/dev/null 2>&1 || fail "Нужен root или sudo."
    SUDO="sudo"
  fi
  command -v apt-get >/dev/null 2>&1 || fail "Поддерживается только Ubuntu/Debian."

  # ---- 1. системные пакеты (ставим только то, чего нет)
  say "[1/5] Системные пакеты"
  local need=()
  command -v git  >/dev/null 2>&1 || need+=(git)
  command -v tmux >/dev/null 2>&1 || need+=(tmux)
  command -v crontab >/dev/null 2>&1 || need+=(cron)
  command -v python3 >/dev/null 2>&1 || need+=(python3)
  python3 -c "import ensurepip, venv" >/dev/null 2>&1 || need+=(python3-venv python3-pip)
  if [ ${#need[@]} -gt 0 ]; then
    # на свежем сервере apt часто занят автообновлениями — ждём до 5 минут
    local APT=(env DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a
               apt-get -o DPkg::Lock::Timeout=300 -y -q)
    $SUDO "${APT[@]}" update >/dev/null
    $SUDO "${APT[@]}" install "${need[@]}" >/dev/null
    ok "установлено: ${need[*]}"
  else
    ok "всё уже есть"
  fi

  # ---- файл подкачки: на сервере с 1–2 ГБ памяти без него система может
  # убить бота при нехватке памяти (например, во время pip install).
  # Создаётся один раз, только если подкачки нет и места на диске хватает.
  if [ -z "$(swapon --show --noheadings 2>/dev/null)" ]; then
    local mem_mb free_mb
    mem_mb=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
    free_mb=$(df -Pm / | awk 'NR==2 {print $4}')
    if [ "${mem_mb:-0}" -lt 4096 ] && [ "${free_mb:-0}" -gt 5120 ] \
       && [ ! -e /swapfile ]; then
      if { $SUDO fallocate -l 2G /swapfile 2>/dev/null \
             || $SUDO dd if=/dev/zero of=/swapfile bs=1M count=2048 status=none; } \
         && $SUDO chmod 600 /swapfile && $SUDO mkswap /swapfile >/dev/null \
         && $SUDO swapon /swapfile; then
        grep -q '^/swapfile ' /etc/fstab \
          || echo '/swapfile none swap sw 0 0' | $SUDO tee -a /etc/fstab >/dev/null
        ok "файл подкачки 2 ГБ создан (память ${mem_mb} МБ)"
      else
        printf '  ! файл подкачки создать не удалось — бот работает и без него\n'
      fi
    fi
  else
    ok "файл подкачки уже есть"
  fi

  # ---- 2. код бота: из GitHub (git) или из скачанной папки; свои файлы
  # пользователя (.env, config.yaml, tickers/, logs/, venv/) не трогаем
  say "[2/5] Код бота"
  if [ -e "$APP_DIR" ] && [ ! -f "$APP_DIR/club.py" ]; then
    fail "Папка $APP_DIR уже есть и это не установка MoneyClub 3. Переименуйте её и запустите снова."
  fi
  if [ -n "$SRC" ]; then
    mkdir -p "$APP_DIR"
    if [ "$SRC" != "$(cd "$APP_DIR" && pwd)" ]; then
      (cd "$SRC" && tar cf - --exclude=./.git --exclude=./.env \
          --exclude=./config.yaml --exclude=./tickers --exclude=./logs \
          --exclude=./venv .) | (cd "$APP_DIR" && tar xf -)
      ok "код скопирован в $APP_DIR"
    else
      ok "установка прямо в этой папке ($APP_DIR)"
    fi
  elif [ -d "$APP_DIR/.git" ]; then
    git -C "$APP_DIR" pull --ff-only -q \
      || fail "Не удалось обновить код в $APP_DIR (git pull)."
    ok "обновлён ($APP_DIR)"
  elif [ -e "$APP_DIR" ]; then
    local TMP
    TMP="$(mktemp -d)"
    git clone -q --depth 1 "$REPO_URL" "$TMP/src" \
      || fail "Не удалось скачать $REPO_URL (репозиторий должен быть публичным)."
    (cd "$TMP/src" && tar cf - --exclude=./.git --exclude=./.env \
        --exclude=./config.yaml --exclude=./tickers --exclude=./logs \
        --exclude=./venv .) | (cd "$APP_DIR" && tar xf -)
    rm -rf "$TMP"
    ok "код обновлён из GitHub ($APP_DIR)"
  else
    git clone -q --depth 1 "$REPO_URL" "$APP_DIR" \
      || fail "Не удалось скачать $REPO_URL (репозиторий должен быть публичным)."
    ok "скачан в $APP_DIR"
  fi
  cd "$APP_DIR"

  # ---- 3. python-окружение и библиотеки
  say "[3/5] Библиотеки (1–3 минуты)"
  if [ ! -x venv/bin/python ]; then
    python3 -m venv venv
  fi
  venv/bin/python -m pip install -q --upgrade pip >/dev/null
  if venv/bin/python -m pip install -q -r requirements-live.txt; then
    ok "установлены, включая боевой режим"
  else
    venv/bin/python -m pip install -q -r requirements.txt \
      || fail "Не удалось установить библиотеки."
    printf '  ! библиотеки боевого режима не встали — тестовая запись работает,\n'
    printf '    боевой режим меню предложит доустановить при старте\n'
  fi

  # ---- 4. файлы настроек (существующие не трогаем)
  say "[4/5] Настройки"
  mkdir -p logs
  if [ ! -f config.yaml ]; then
    cp config.example.yaml config.yaml
    ok "config.yaml создан (пороги -7.0 / 4.0 / 4.5, лимит \$25)"
  else
    ok "config.yaml уже есть — оставлен как был"
  fi
  if [ ! -f .env ]; then
    ok "ключи введёте в меню"
  else
    chmod 600 .env
    ok ".env уже есть — ключи сохранены"
  fi

  # ---- 5. команда moneyclub. Если она была у MoneyClub версии 2 (другая
  # папка), её команда становится moneyclub2 — тот бот остаётся доступен.
  say "[5/5] Команда $CMD"
  local BIN="/usr/local/bin/$CMD"
  if [ -f "$BIN" ] && ! grep -qF "$APP_DIR/club.py" "$BIN"; then
    if [ ! -e "${BIN}2" ]; then
      $SUDO mv "$BIN" "${BIN}2"
      ok "прежний бот с командой $CMD теперь открывается командой ${CMD}2"
    else
      printf '  ! команда %s заменена (команда %s2 уже была)\n' "$CMD" "$CMD"
    fi
  fi
  local LAUNCHER
  LAUNCHER="$(mktemp)"
  cat > "$LAUNCHER" <<EOF
#!/usr/bin/env bash
# MoneyClub версия 3 — открыть меню
export PYTHONUTF8=1
cd "$APP_DIR" && exec "$APP_DIR/venv/bin/python" "$APP_DIR/club.py" "\$@"
EOF
  chmod 755 "$LAUNCHER"
  $SUDO mv "$LAUNCHER" "$BIN"
  ok "меню открывается командой: $CMD"

  # ---- меню при входе на сервер по SSH. Старые блоки автозапуска (moneyclub
  # версии 2, volume_earningbot) убираются, чтобы при входе не открывались
  # два меню подряд; их команды остаются.
  local RC="$HOME/.bashrc"
  touch "$RC"
  local tag
  for tag in moneyclub volume_earningbot moneyclub3; do
    if grep -q ">>> $tag >>>" "$RC"; then
      sed -i "/>>> $tag >>>/,/<<< $tag <<</d" "$RC"
      [ "$tag" = moneyclub3 ] || ok "старое автооткрытие меню ($tag) при входе снято"
    fi
  done
  cat >> "$RC" <<EOF
# >>> moneyclub3 >>>  (меню при входе по SSH; отключить: удалите этот блок)
if [ -n "\$SSH_TTY" ] && [ -z "\$TMUX" ] && [ -z "\$MONEYCLUB_NOAUTO" ] \\
   && [[ \$- == *i* ]] && command -v $CMD >/dev/null 2>&1; then
  $CMD
fi
# <<< moneyclub3 <<<
EOF
  ok "меню будет открываться само при входе на сервер"

  # ---- автоперезапуск: после перезагрузки сервера и если бот упал
  # (cron раз в 5 минут вызывает "$CMD --resume"; бот поднимается, только
  # если его запускали из меню и не останавливали Стопом)
  if command -v crontab >/dev/null 2>&1; then
    local CRON_TAG="# moneyclub3 autorun ($APP_DIR)"
    { crontab -l 2>/dev/null | grep -vF "$CRON_TAG" || true
      echo "@reboot sleep 60; /usr/local/bin/$CMD --resume >/dev/null 2>&1 $CRON_TAG"
      echo "*/5 * * * * /usr/local/bin/$CMD --resume >/dev/null 2>&1 $CRON_TAG"
    } | crontab -
    ok "автоперезапуск: после перезагрузки сервера и при сбое бота"
  else
    printf '  ! cron не найден — автоперезапуска не будет\n'
  fi

  printf '\n  Готово.\n\n'
  sleep 1

  # запуск меню: при "curl | bash" клавиатура доступна только через /dev/tty
  if (exec </dev/tty) 2>/dev/null; then
    exec "/usr/local/bin/$CMD" </dev/tty
  else
    printf '  Откройте меню командой: %s\n\n' "$CMD"
  fi
}

main "$@"
