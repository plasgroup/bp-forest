#!/usr/bin/env python3
"""リンク済み DPU ELF の overlay セグメントをロードアドレス (LMA) へ移す。

dpu_load は VMA だけを見てロード先メモリを決め、LMA (p_paddr) を無視する。
そこで p_vaddr != p_paddr の LOAD セグメント (= overlay) の p_vaddr と
所属セクションの sh_addr を LMA に書き換え、MRAM に配置させる。再配置は
リンク時に解決済みなので実行内容には影響しない。
"""
import struct
import sys

PT_LOAD = 1
SHF_ALLOC = 0x2


def patch(path):
    with open(path, "rb") as f:
        elf = bytearray(f.read())

    assert elf[:4] == b"\x7fELF" and elf[4] == 1 and elf[5] == 1, "not a 32-bit LE ELF"
    e_phoff, = struct.unpack_from("<I", elf, 0x1C)
    e_shoff, = struct.unpack_from("<I", elf, 0x20)
    e_phentsize, e_phnum, e_shentsize, e_shnum = struct.unpack_from("<HHHH", elf, 0x2A)

    moved = []  # (p_offset, p_filesz, p_paddr)
    for i in range(e_phnum):
        off = e_phoff + i * e_phentsize
        p_type, p_offset, p_vaddr, p_paddr, p_filesz = struct.unpack_from(
            "<IIIII", elf, off)
        if p_type == PT_LOAD and p_vaddr != p_paddr:
            struct.pack_into("<I", elf, off + 8, p_paddr)  # p_vaddr = p_paddr
            moved.append((p_offset, p_filesz, p_paddr))
            print(f"segment {i}: vaddr 0x{p_vaddr:08x} -> lma 0x{p_paddr:08x}"
                  f" ({p_filesz} bytes)")

    for i in range(e_shnum):
        off = e_shoff + i * e_shentsize
        sh_flags, sh_addr, sh_offset = struct.unpack_from("<III", elf, off + 8)
        if not (sh_flags & SHF_ALLOC):
            continue
        for p_offset, p_filesz, p_paddr in moved:
            if p_offset <= sh_offset < p_offset + p_filesz:
                struct.pack_into("<I", elf, off + 12,
                                 p_paddr + (sh_offset - p_offset))

    if not moved:
        print("no overlay segments (vaddr != paddr) found")

    with open(path, "wb") as f:
        f.write(elf)


if __name__ == "__main__":
    patch(sys.argv[1])
