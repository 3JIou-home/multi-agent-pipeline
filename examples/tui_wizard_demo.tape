Output examples/tui_wizard_demo.gif

Require bash
Require rm
Require mkdir
Require pwd
Require vhs

Set Shell "bash"
Set FontSize 24
Set Width 1600
Set Height 1000
Set TypingSpeed 0ms
Set Theme "Builtin Solarized Dark"

Hide
Type "rm -rf /tmp/agpipe-vhs-runs /tmp/agpipe-vhs-workspace && mkdir -p /tmp/agpipe-vhs-runs /tmp/agpipe-vhs-workspace"
Enter
Sleep 500ms
Show

Type "AGPIPE_STAGE0_BACKEND=local AGPIPE_CODEX_EPHEMERAL=1 AGPIPE_CODEX_BIN=$(pwd)/examples/mock-codex-vhs.zsh ./target/release/agpipe ui --root /tmp/agpipe-vhs-runs"
Enter
Sleep 1500ms

Type "c"
Sleep 500ms

Type "Сделай CLI на Python для обработки текстовых файлов, но сначала уточни у меня сценарий использования, аргументы и ожидаемый вывод."
Tab
Type "/tmp/agpipe-vhs-workspace"
Tab
Type "vhs-demo"
Tab
Enter
Sleep 1500ms

Type "Нужны простой CLI, понятные аргументы, README с примером запуска и предсказуемый текстовый вывод."
Enter
Sleep 1500ms

Tab
Enter
Sleep 6000ms

Type "q"
Sleep 1000ms
