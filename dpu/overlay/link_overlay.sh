#!/usr/bin/env bash
#
# Overlay-aware link of the DPU program (invoked as CMAKE_C_LINK_EXECUTABLE
# in the form "<driver> <link args...>").
#
# The SDK driver always passes its own -T dpu.lds, so this script extracts
# the link command from the -### output, generates "the lds to use" from that
# dpu.lds, and runs the command with -T replaced. Generation has two steps:
# (1) formally widen the LENGTH of the iram region (lld adds up the sizes of
# all overlay members even though their VMAs overlap, and reports a false
# overflow; the real capacity is checked by the ASSERTs in
# overlay_additions.lds), and (2) append the overlay definitions
# (overlay_additions.lds).
# Finally, move the overlay segments to their LMA.
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
