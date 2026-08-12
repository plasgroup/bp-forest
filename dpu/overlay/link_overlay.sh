#!/usr/bin/env bash
#
# DPU プログラムの overlay 対応リンク (CMAKE_C_LINK_EXECUTABLE として
# 「<ドライバ> <リンク引数...>」の形で呼ばれる)。
#
# SDK ドライバは常に自前の -T dpu.lds を渡すため、-### の出力からリンク
# コマンドを抽出し、その dpu.lds から「使う lds」を生成して -T を差し替えて
# 実行する。生成は 2 段: (1) iram 領域の LENGTH を形式上広げる (lld は
# overlay の各メンバを VMA が重なっていても累積で数えて偽の容量超過に
# するため。実容量は overlay_additions.lds の ASSERT で検査する)、
# (2) overlay の追加定義 (overlay_additions.lds) を連結する。
# 最後に overlay セグメントを LMA へ移す。
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

driver=$1
shift

target=
prev=
for arg in "$@"; do
    if [ "$prev" = "-o" ]; then
        target=$arg
    fi
    prev=$arg
done
if [ -z "$target" ]; then
    echo "ERROR: no -o <target> in link arguments" >&2
    exit 1
fi

link_cmd=$("$driver" -### "$@" 2>&1 | tail -1)
case "$link_cmd" in
*ld.lld*--defsym=NR_TASKLETS*) ;;
*) echo "ERROR: could not extract link command from driver output:" >&2
   echo "$link_cmd" >&2
   exit 1 ;;
esac

orig_lds=$(printf '%s' "$link_cmd" | sed -n 's/.*"-T" "\([^"]*\)".*/\1/p')
if [ ! -f "$orig_lds" ]; then
    echo "ERROR: linker script not found in link command: $orig_lds" >&2
    exit 1
fi

gen_lds=$target.lds
sed 's/LENGTH = DPU_IRAM_SIZE/LENGTH = 1M/' "$orig_lds" > "$gen_lds"
if ! grep -q 'LENGTH = 1M' "$gen_lds"; then
    echo "ERROR: could not widen the iram region in $orig_lds" >&2
    exit 1
fi
cat "$here/overlay_additions.lds" >> "$gen_lds"

link_cmd=$(printf '%s' "$link_cmd" | sed "s|\"-T\" \"[^\"]*\"|\"-T\" \"$gen_lds\"|")
eval "$link_cmd"

python3 "$here/apply_overlay_lma.py" "$target" > /dev/null
