#include "partition.hpp"

#include <cmdline.h>

#include <cstddef>
#include <optional>
#include <cstdint>
#include <string>


namespace cmdline
{
template <typename T>
struct default_reader<std::optional<T>> {
    std::optional<T> operator()(const std::string& str)
    {
        return default_reader<T>{}(str);
    }
};
namespace detail
{
template <typename T>
class lexical_cast_t<std::string, std::optional<T>, false>
{
public:
    static std::string cast(const std::optional<T>& opt)
    {
        return opt ? lexical_cast<std::string>(*opt) : "(nullopt)";
    }
};
}  // namespace detail
}  // namespace cmdline


struct CMDOpt {
    std::string partition_file;
    std::optional<uint32_t> base;

    CMDOpt(int argc, char* argv[])
    {
        cmdline::parser parser;
        parser.add<std::string>("partition", 'p', "file path to pre-calculated partitioning", true);
        parser.add<std::optional<uint32_t>>("base", 0, "show key range of base partition for specified index", false);
        parser.parse_check(argc, argv);

        partition_file = parser.get<std::string>("partition");
        base = parser.get<std::optional<uint32_t>>("base");
    }
};

int main(int argc, char* argv[])
{
    const CMDOpt opt{argc, argv};
    const auto partitions = load_partition(opt.partition_file);
    if (opt.base) {
        if (*opt.base >= partitions.size() / 2) {
            std::cerr << "base partition index is out of range" << std::endl;
            return 1;
        }
        const Partition& part = partitions[*opt.base];
        const int64_t left_key = part.left_key;
        const int64_t right_key = static_cast<int64_t>(static_cast<uint64_t>(left_key) + part.length - 1u);
        std::cout << left_key << "," << right_key << std::endl;
    }
    (void)opt;
    return 0;
}
