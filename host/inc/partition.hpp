#include <cstdint>
#include <vector>
#include <string>

struct partition {
    int64_t left_key;
    size_t length;
};

void store_partition(const std::string& file_name, const std::vector<partition>& partition)
{
    FILE* fp = fopen(file_name.c_str(), "wb");
    if (!fp) {
        perror("fopen");
        exit(1);
    }
    for (auto p: partition) {
        fwrite(&p, sizeof(p), 1, fp);
    }
    fclose(fp);
}