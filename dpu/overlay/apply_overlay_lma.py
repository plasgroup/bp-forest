#!/usr/bin/env python3
"""Move the overlay segments of a linked DPU ELF to their load address (LMA).

dpu_load decides the destination memory from the VMA alone and ignores the
LMA (p_paddr). This script therefore rewrites p_vaddr of every LOAD segment
with p_vaddr != p_paddr (= an overlay), and sh_addr of the sections in it, to
the LMA, so that they are placed in MRAM. Relocations were already resolved
at link time, so this does not affect what is executed.
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
