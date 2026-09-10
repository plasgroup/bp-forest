// RESP2 (Redis serialization protocol) server front-end for BPForest.
//
// Bridges stream-oriented Redis clients to BPForest's batch-only API: commands
// pipelined by all connections are accumulated into an "epoch", each epoch is
// executed as one round of batch_* calls, and the replies are written back in
// per-connection arrival order.  Deep client-side pipelining is therefore the
// intended usage; a lone unpipelined command still works but pays the whole
// epoch latency for itself.
//
// Within one epoch, reads (GET/EXISTS/BPF.*) execute before inserts (SET),
// which execute before deletes (DEL).  To keep each connection sequentially
// consistent under this fixed order, an epoch admits from one connection only
// a contiguous run of commands of the same class; the first command of a
// different class waits for the next epoch.  Commands of different
// connections in the same epoch have no defined relative order (Redis makes
// no promise across clients either).
//
// Protocol caveats are documented in resp_server/README.md.

#include "bpforest.hpp"
#include "common.h"
#include "database.hpp"
#include "host_params.hpp"
#include "overload_threshold.hpp"
#include "workload_types.h"

#include <cmdline.h>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <numa.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>


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


namespace
{

volatile std::sig_atomic_t g_stop = 0;
void handle_stop_signal(int) { g_stop = 1; }


//! @return whether `s` is a decimal uint64 with no sign, space, nor prefix
bool parse_u64(const std::string& s, uint64_t* out)
{
    if (s.empty() || s.size() > 20)
        return false;
    uint64_t v = 0;
    for (const char ch : s) {
        if (ch < '0' || ch > '9')
            return false;
        const unsigned d = static_cast<unsigned>(ch - '0');
        if (v > (std::numeric_limits<uint64_t>::max() - d) / 10)
            return false;
        v = v * 10 + d;
    }
    *out = v;
    return true;
}

std::string upper_copy(const std::string& s)
{
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), [](unsigned char c) { return std::toupper(c); });
    return r;
}


/* RESP2 reply builders, appending to a connection's output buffer */

constexpr char NIL_BULK[] = "$-1\r\n";
constexpr char NIL_ARRAY[] = "*-1\r\n";

void put_simple(std::string& out, const char* s)
{
    out += '+';
    out += s;
    out += "\r\n";
}
void put_error(std::string& out, const std::string& msg)
{
    out += '-';
    out += msg;
    out += "\r\n";
}
void put_int(std::string& out, uint64_t v)
{
    out += ':';
    out += std::to_string(v);
    out += "\r\n";
}
void put_bulk(std::string& out, const std::string& s)
{
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}
void put_bulk_u64(std::string& out, uint64_t v)
{
    put_bulk(out, std::to_string(v));
}


/* incremental RESP2 request parser */

enum class ParseStatus { Complete, Incomplete, Malformed };

//! Parses "<digits>\r\n" at `buf[*cur ..]`, advancing `*cur` past the CRLF.
ParseStatus parse_number_line(const char* buf, size_t len, size_t* cur, uint64_t* out)
{
    uint64_t v = 0;
    size_t p = *cur;
    const size_t digits_end = std::min(len, *cur + 16);
    for (; p < digits_end && buf[p] >= '0' && buf[p] <= '9'; p++)
        v = v * 10 + static_cast<unsigned>(buf[p] - '0');
    if (p == *cur)
        return p < len ? ParseStatus::Malformed : ParseStatus::Incomplete;
    if (p + 2 > len)
        return p >= *cur + 16 ? ParseStatus::Malformed : ParseStatus::Incomplete;
    if (buf[p] != '\r' || buf[p + 1] != '\n')
        return ParseStatus::Malformed;
    *cur = p + 2;
    *out = v;
    return ParseStatus::Complete;
}

//! Parses one command (RESP array of bulk strings, or an inline line) out of
//! `buf[0 .. len)`.  On Complete, `*consumed` is the parsed byte count and
//! `*args` the arguments; an empty `*args` means a blank inline line to skip.
ParseStatus parse_command(const char* buf, size_t len, size_t* consumed, std::vector<std::string>* args)
{
    constexpr size_t MAX_ARGS = 1024, MAX_BULK_LEN = 1u << 20, MAX_INLINE_LEN = 1u << 16;

    args->clear();
    if (len == 0)
        return ParseStatus::Incomplete;

    if (buf[0] != '*') {  // inline command: one space-separated line
        const char* nl = static_cast<const char*>(std::memchr(buf, '\n', std::min(len, MAX_INLINE_LEN)));
        if (nl == nullptr)
            return len >= MAX_INLINE_LEN ? ParseStatus::Malformed : ParseStatus::Incomplete;
        size_t line_len = static_cast<size_t>(nl - buf);
        *consumed = line_len + 1;
        if (line_len > 0 && buf[line_len - 1] == '\r')
            line_len--;
        size_t p = 0;
        while (p < line_len) {
            while (p < line_len && buf[p] == ' ')
                p++;
            const size_t begin = p;
            while (p < line_len && buf[p] != ' ')
                p++;
            if (p > begin)
                args->emplace_back(buf + begin, p - begin);
        }
        return ParseStatus::Complete;
    }

    size_t cur = 1;
    uint64_t nr_args;
    switch (parse_number_line(buf, len, &cur, &nr_args)) {
        case ParseStatus::Complete:
            break;
        case ParseStatus::Incomplete:
            return ParseStatus::Incomplete;
        case ParseStatus::Malformed:
            return ParseStatus::Malformed;
    }
    if (nr_args < 1 || nr_args > MAX_ARGS)
        return ParseStatus::Malformed;

    args->reserve(nr_args);
    for (uint64_t i = 0; i < nr_args; i++) {
        if (cur >= len)
            return ParseStatus::Incomplete;
        if (buf[cur] != '$')
            return ParseStatus::Malformed;
        cur++;
        uint64_t bulk_len;
        switch (parse_number_line(buf, len, &cur, &bulk_len)) {
            case ParseStatus::Complete:
                break;
            case ParseStatus::Incomplete:
                return ParseStatus::Incomplete;
            case ParseStatus::Malformed:
                return ParseStatus::Malformed;
        }
        if (bulk_len > MAX_BULK_LEN)
            return ParseStatus::Malformed;
        if (cur + bulk_len + 2 > len)
            return ParseStatus::Incomplete;
        if (buf[cur + bulk_len] != '\r' || buf[cur + bulk_len + 1] != '\n')
            return ParseStatus::Malformed;
        args->emplace_back(buf + cur, bulk_len);
        cur += bulk_len + 2;
    }
    *consumed = cur;
    return ParseStatus::Complete;
}


/* epoch accumulation */

enum class CmdClass { None, Read, Insert, Delete };

//! Queries admitted to the current epoch, in per-operation arrays that are
//! handed to the batch_* calls as-is.  SET and DEL are deduplicated by key;
//! the last admitted SET of a key wins.
struct Epoch {
    std::vector<key_uint64_t> get_keys;  // GET, and existence probes of EXISTS
    std::vector<value_int64_t> get_results;
    std::vector<key_uint64_t> pred_keys;
    std::vector<KVPair> pred_results;
    std::vector<RangeCountQuery> count_qrys;
    std::vector<uint64_t> count_results;
    std::vector<KeyRange> max_qrys;
    std::vector<value_int64_t> max_results;
    std::vector<KVPair> ins_pairs;
    std::unordered_map<key_uint64_t, uint32_t> ins_pos;
    std::vector<key_uint64_t> del_keys;
    std::vector<uint8_t> del_results;
    std::unordered_set<key_uint64_t> del_set;

    bool empty() const
    {
        return get_keys.empty() && pred_keys.empty() && count_qrys.empty()
               && max_qrys.empty() && ins_pairs.empty() && del_keys.empty();
    }
    void clear()
    {
        get_keys.clear();
        get_results.clear();
        pred_keys.clear();
        pred_results.clear();
        count_qrys.clear();
        count_results.clear();
        max_qrys.clear();
        max_results.clear();
        ins_pairs.clear();
        ins_pos.clear();
        del_keys.clear();
        del_results.clear();
        del_set.clear();
    }
};

//! One queued reply of a connection.  Text replies are pre-formatted at
//! admission; the others are resolved from the epoch results (DbSize from the
//! host-side pair counters) after the epoch executes.
struct Reply {
    enum class Kind : uint8_t { Text, Get, Exists, Del, Pred, Count, Max, DbSize };

    Kind kind;
    uint32_t idx = 0;   // index into the corresponding epoch array
    uint32_t n = 0;     // #epoch-array entries (Exists probes / Del keys)
    std::string text;   // Text
};

struct Conn {
    int fd;
    std::vector<char> in;
    size_t in_off = 0;
    std::deque<std::vector<std::string>> cmds;  // parsed but not yet admitted
    std::deque<Reply> replies;                  // admitted; drained in order as they resolve
    std::string out;
    size_t out_off = 0;
    CmdClass cls = CmdClass::None;  // class of the run admitted to the current epoch
    bool close_after_flush = false;
    bool read_eof = false;
    bool dead = false;
    bool epollout = false;

    explicit Conn(int fd_) : fd{fd_} {}
};


class RespServer
{
public:
    struct Options {
        std::string bind_addr = "127.0.0.1";
        int port = 6399;
        size_t epoch_cap = 1u << 20;     // max queries admitted to one epoch
        size_t max_pipeline = 1u << 20;  // max parsed-but-unadmitted commands per connection
    };

    RespServer(BPForest& forest, const Options& opt)
        : forest_{forest}, opt_{opt}
    {
        listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (listen_fd_ < 0)
            throw std::system_error{errno, std::generic_category(), "socket"};
        const int one = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(opt.port));
        if (inet_pton(AF_INET, opt.bind_addr.c_str(), &addr.sin_addr) != 1)
            throw std::runtime_error{"invalid bind address: " + opt.bind_addr};
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0)
            throw std::system_error{errno, std::generic_category(), "bind"};
        if (listen(listen_fd_, 1024) != 0)
            throw std::system_error{errno, std::generic_category(), "listen"};

        epfd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epfd_ < 0)
            throw std::system_error{errno, std::generic_category(), "epoll_create1"};
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) != 0)
            throw std::system_error{errno, std::generic_category(), "epoll_ctl(listen)"};
    }

    ~RespServer()
    {
        for (auto& [fd, conn] : conns_)
            close(fd);
        close(epfd_);
        close(listen_fd_);
    }

    void run()
    {
        while (!g_stop && !shutdown_cmd_) {
            epoll_event events[128];
            const int timeout_ms = have_pending_work() ? 0 : -1;
            const int n = epoll_wait(epfd_, events, 128, timeout_ms);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                throw std::system_error{errno, std::generic_category(), "epoll_wait"};
            }
            for (int i = 0; i < n; i++) {
                if (events[i].data.fd == listen_fd_)
                    accept_conns();
                else
                    handle_event(events[i]);
            }

            admit_all();
            if (!epoch_.empty() || nr_pending_replies_ > 0) {
                execute_epoch();
                resolve_replies();
                epoch_.clear();
                nr_epoch_queries_ = 0;
            }
            emit_ready_replies();
            flush_all();
            reap_dead_conns();
        }
    }

private:
    bool have_pending_work() const
    {
        if (!epoch_.empty() || nr_pending_replies_ > 0)
            return true;
        for (const auto& [fd, conn] : conns_)
            if (!conn->cmds.empty() || conn->out_off < conn->out.size() || conn->dead)
                return true;
        return false;
    }

    void accept_conns()
    {
        for (;;) {
            const int fd = accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
                    return;
                if (errno == EMFILE || errno == ENFILE || errno == ECONNABORTED)
                    return;
                throw std::system_error{errno, std::generic_category(), "accept4"};
            }
            const int one = 1;
            setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
            epoll_event ev{};
            ev.events = EPOLLIN;
            ev.data.fd = fd;
            if (epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) != 0) {
                close(fd);
                continue;
            }
            conns_.emplace(fd, std::make_unique<Conn>(fd));
        }
    }

    void handle_event(const epoll_event& ev)
    {
        const auto it = conns_.find(ev.data.fd);
        if (it == conns_.end())
            return;
        Conn& c = *it->second;
        if (ev.events & (EPOLLHUP | EPOLLERR)) {
            c.dead = true;
            return;
        }
        if (ev.events & EPOLLIN)
            read_conn(c);
        if (ev.events & EPOLLOUT)
            flush_conn(c);
    }

    void read_conn(Conn& c)
    {
        if (c.read_eof || c.close_after_flush)
            return;
        char tmp[65536];
        while (c.cmds.size() < opt_.max_pipeline) {
            const ssize_t r = read(c.fd, tmp, sizeof tmp);
            if (r > 0) {
                c.in.insert(c.in.end(), tmp, tmp + r);
                if (static_cast<size_t>(r) < sizeof tmp)
                    break;  // likely drained; avoid an extra syscall
            } else if (r == 0) {
                c.read_eof = true;
                break;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else if (errno != EINTR) {
                c.dead = true;
                return;
            }
        }
        parse_conn(c);
    }

    void parse_conn(Conn& c)
    {
        std::vector<std::string> args;
        while (c.cmds.size() < opt_.max_pipeline) {
            size_t consumed = 0;
            const ParseStatus st = parse_command(c.in.data() + c.in_off, c.in.size() - c.in_off, &consumed, &args);
            if (st == ParseStatus::Complete) {
                c.in_off += consumed;
                if (!args.empty())
                    c.cmds.push_back(std::move(args));
            } else if (st == ParseStatus::Incomplete) {
                break;
            } else {
                put_error(c.out, "ERR Protocol error");
                c.close_after_flush = true;
                c.cmds.clear();
                break;
            }
        }
        if (c.in_off == c.in.size()) {
            c.in.clear();
            c.in_off = 0;
        } else if (c.in_off > (1u << 20)) {
            c.in.erase(c.in.begin(), c.in.begin() + static_cast<ptrdiff_t>(c.in_off));
            c.in_off = 0;
        }
    }

    /* admission: move commands into the epoch under the same-class-run rule */

    void admit_all()
    {
        for (auto& [fd, conn] : conns_)
            if (!conn->dead)
                admit_conn(*conn);
    }

    void admit_conn(Conn& c)
    {
        while (!c.cmds.empty() && !c.close_after_flush) {
            if (nr_epoch_queries_ >= opt_.epoch_cap)
                return;
            if (!admit_command(c, c.cmds.front()))
                return;  // class cut: the command waits for the next epoch
            c.cmds.pop_front();
        }
    }

    //! @return whether the command was consumed (admitted or answered)
    bool admit_command(Conn& c, const std::vector<std::string>& args)
    {
        const std::string cmd = upper_copy(args[0]);

        /* instant commands: never touch the epoch, never cut a run */
        if (cmd == "PING") {
            if (args.size() == 1)
                push_text(c, [](std::string& o) { put_simple(o, "PONG"); });
            else if (args.size() == 2)
                push_text(c, [&](std::string& o) { put_bulk(o, args[1]); });
            else
                push_arity_error(c, cmd);
            return true;
        }
        if (cmd == "ECHO") {
            if (args.size() == 2)
                push_text(c, [&](std::string& o) { put_bulk(o, args[1]); });
            else
                push_arity_error(c, cmd);
            return true;
        }
        if (cmd == "COMMAND" || cmd == "CONFIG") {
            // enough for redis-cli & friends to connect
            push_text(c, [](std::string& o) { o += "*0\r\n"; });
            return true;
        }
        if (cmd == "DBSIZE") {
            c.replies.push_back(Reply{Reply::Kind::DbSize});
            nr_pending_replies_++;
            return true;
        }
        if (cmd == "QUIT") {
            push_text(c, [](std::string& o) { put_simple(o, "OK"); });
            c.close_after_flush = true;  // admit_conn stops here; leftovers die with the conn
            return true;
        }
        if (cmd == "SHUTDOWN") {
            shutdown_cmd_ = true;
            c.close_after_flush = true;
            return true;
        }

        /* epoch-bound commands */
        if (cmd == "GET")
            return admit_get(c, cmd, args);
        if (cmd == "EXISTS")
            return admit_exists(c, cmd, args);
        if (cmd == "SET")
            return admit_set(c, cmd, args);
        if (cmd == "DEL")
            return admit_del(c, cmd, args);
        if (cmd == "BPF.PRED")
            return admit_pred(c, cmd, args);
        if (cmd == "BPF.RANGECOUNT")
            return admit_range_count(c, cmd, args);
        if (cmd == "BPF.RANGEMAX")
            return admit_range_max(c, cmd, args);

        push_text(c, [&](std::string& o) { put_error(o, "ERR unknown command '" + args[0] + "'"); });
        return true;
    }

    bool class_allows(Conn& c, CmdClass k)
    {
        if (c.cls != CmdClass::None && c.cls != k)
            return false;
        c.cls = k;
        return true;
    }

    template <typename F>
    void push_text(Conn& c, F&& format)
    {
        Reply r{Reply::Kind::Text};
        format(r.text);
        c.replies.push_back(std::move(r));
        nr_pending_replies_++;
    }
    void push_arity_error(Conn& c, const std::string& cmd)
    {
        push_text(c, [&](std::string& o) { put_error(o, "ERR wrong number of arguments for '" + cmd + "' command"); });
    }
    // The DPU program only contains the tasks selected by the SUPPORT_* build
    // flags; commands whose task is missing must be rejected here, before
    // they can reach a DPU (executing a missing task kills the DPU program).
    [[maybe_unused]] void push_unsupported(Conn& c, const std::string& cmd)
    {
        push_text(c, [&](std::string& o) { put_error(o, "ERR command '" + cmd + "' is not supported by this build"); });
    }
    void push_key_error(Conn& c)
    {
        push_text(c, [](std::string& o) { put_error(o, "ERR keys and values must be decimal unsigned 64-bit integers"); });
    }

    bool admit_get(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_GET
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() != 2) {
            push_arity_error(c, cmd);
            return true;
        }
        uint64_t key;
        if (!parse_u64(args[1], &key)) {
            push_key_error(c);
            return true;
        }
        if (!class_allows(c, CmdClass::Read))
            return false;
        c.replies.push_back(Reply{Reply::Kind::Get, static_cast<uint32_t>(epoch_.get_keys.size())});
        nr_pending_replies_++;
        epoch_.get_keys.push_back(key);
        nr_epoch_queries_++;
        return true;
#endif
    }

    bool admit_exists(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_GET  // EXISTS is answered with get probes
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() < 2) {
            push_arity_error(c, cmd);
            return true;
        }
        std::vector<uint64_t> keys(args.size() - 1);
        for (size_t i = 1; i < args.size(); i++) {
            if (!parse_u64(args[i], &keys[i - 1])) {
                push_key_error(c);
                return true;
            }
        }
        if (!class_allows(c, CmdClass::Read))
            return false;
        c.replies.push_back(Reply{Reply::Kind::Exists,
            static_cast<uint32_t>(epoch_.get_keys.size()), static_cast<uint32_t>(keys.size())});
        nr_pending_replies_++;
        epoch_.get_keys.insert(epoch_.get_keys.end(), keys.begin(), keys.end());
        nr_epoch_queries_ += keys.size();
        return true;
#endif
    }

    bool admit_set(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_INSERT
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() != 3) {
            push_arity_error(c, cmd);
            return true;
        }
        uint64_t key, value;
        if (!parse_u64(args[1], &key) || !parse_u64(args[2], &value)) {
            push_key_error(c);
            return true;
        }
        if (value > static_cast<uint64_t>(VALUE_MAX)) {
            push_text(c, [](std::string& o) {
                put_error(o, "ERR value out of range");
            });
            return true;
        }
        if (!class_allows(c, CmdClass::Insert))
            return false;
        const auto [it, inserted] = epoch_.ins_pos.try_emplace(key, static_cast<uint32_t>(epoch_.ins_pairs.size()));
        if (inserted)
            epoch_.ins_pairs.push_back(KVPair{key, static_cast<value_int64_t>(value)});
        else
            epoch_.ins_pairs[it->second].value = static_cast<value_int64_t>(value);  // last SET in the epoch wins
        nr_epoch_queries_++;
        push_text(c, [](std::string& o) { put_simple(o, "OK"); });
        return true;
#endif
    }

    bool admit_del(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_DELETE
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() < 2) {
            push_arity_error(c, cmd);
            return true;
        }
        std::vector<uint64_t> keys;
        keys.reserve(args.size() - 1);
        for (size_t i = 1; i < args.size(); i++) {
            uint64_t key;
            if (!parse_u64(args[i], &key)) {
                push_key_error(c);
                return true;
            }
            if (std::find(keys.begin(), keys.end(), key) == keys.end())
                keys.push_back(key);  // count duplicated arguments once, like Redis
        }
        if (!class_allows(c, CmdClass::Delete))
            return false;
        // batch_delete reports per key whether a live pair was deleted, which
        // is exactly this reply's count.  Keys already deleted earlier in the
        // epoch are not re-sent (del_set): they surely contribute 0, and the
        // first deleter must keep the credit for the serial order.
        const uint32_t idx_keys = static_cast<uint32_t>(epoch_.del_keys.size());
        uint32_t nr_new_keys = 0;
        for (const uint64_t key : keys) {
            if (epoch_.del_set.insert(key).second) {
                epoch_.del_keys.push_back(key);
                nr_new_keys++;
            }
        }
        c.replies.push_back(Reply{Reply::Kind::Del, idx_keys, nr_new_keys});
        nr_pending_replies_++;
        nr_epoch_queries_ += nr_new_keys;
        return true;
#endif
    }

    bool admit_pred(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_PRED
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() != 2) {
            push_arity_error(c, cmd);
            return true;
        }
        uint64_t key;
        if (!parse_u64(args[1], &key)) {
            push_key_error(c);
            return true;
        }
        if (!class_allows(c, CmdClass::Read))
            return false;
        c.replies.push_back(Reply{Reply::Kind::Pred, static_cast<uint32_t>(epoch_.pred_keys.size())});
        nr_pending_replies_++;
        epoch_.pred_keys.push_back(key);
        nr_epoch_queries_++;
        return true;
#endif
    }

    bool admit_range_count(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_RANGE_COUNT
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() != 4) {
            push_arity_error(c, cmd);
            return true;
        }
        uint64_t begin, end, needle;
        if (!parse_u64(args[1], &begin) || !parse_u64(args[2], &end) || !parse_u64(args[3], &needle)) {
            push_key_error(c);
            return true;
        }
        if (begin > end) {
            push_text(c, [](std::string& o) { put_int(o, 0); });
            return true;
        }
        if (!class_allows(c, CmdClass::Read))
            return false;
        c.replies.push_back(Reply{Reply::Kind::Count, static_cast<uint32_t>(epoch_.count_qrys.size())});
        nr_pending_replies_++;
        epoch_.count_qrys.push_back(RangeCountQuery{KeyRange{begin, end}, static_cast<value_int64_t>(needle)});
        nr_epoch_queries_++;
        return true;
#endif
    }

    bool admit_range_max(Conn& c, const std::string& cmd, const std::vector<std::string>& args)
    {
#ifndef SUPPORT_RANGE_MAX
        (void)args;
        push_unsupported(c, cmd);
        return true;
#else
        if (args.size() != 3) {
            push_arity_error(c, cmd);
            return true;
        }
        uint64_t begin, end;
        if (!parse_u64(args[1], &begin) || !parse_u64(args[2], &end)) {
            push_key_error(c);
            return true;
        }
        if (begin > end) {
            push_text(c, [](std::string& o) { o += NIL_BULK; });
            return true;
        }
        if (!class_allows(c, CmdClass::Read))
            return false;
        c.replies.push_back(Reply{Reply::Kind::Max, static_cast<uint32_t>(epoch_.max_qrys.size())});
        nr_pending_replies_++;
        epoch_.max_qrys.push_back(KeyRange{begin, end});
        nr_epoch_queries_++;
        return true;
#endif
    }

    /* epoch execution and reply resolution */

    void execute_epoch()
    {
        Epoch& ep = epoch_;
        if (!ep.get_keys.empty()) {
            ep.get_results.resize(ep.get_keys.size());
            forest_.batch_get(static_cast<uint32_t>(ep.get_keys.size()), ep.get_keys.data(), ep.get_results.data());
        }
        if (!ep.pred_keys.empty()) {
            ep.pred_results.resize(ep.pred_keys.size());
            forest_.batch_pred(static_cast<uint32_t>(ep.pred_keys.size()), ep.pred_keys.data(), ep.pred_results.data());
        }
        if (!ep.count_qrys.empty()) {
            ep.count_results.resize(ep.count_qrys.size());
            forest_.batch_range_count(static_cast<uint32_t>(ep.count_qrys.size()), ep.count_qrys.data(), ep.count_results.data());
        }
        if (!ep.max_qrys.empty()) {
            ep.max_results.resize(ep.max_qrys.size());
            forest_.batch_range_max(static_cast<uint32_t>(ep.max_qrys.size()), ep.max_qrys.data(), ep.max_results.data());
        }
        if (!ep.ins_pairs.empty())
            forest_.batch_insert(static_cast<uint32_t>(ep.ins_pairs.size()), ep.ins_pairs.data());
        if (!ep.del_keys.empty()) {
            ep.del_results.resize(ep.del_keys.size());
            forest_.batch_delete(static_cast<uint32_t>(ep.del_keys.size()), ep.del_keys.data(), ep.del_results.data());
        }
    }

    //! Resolves every reply the just-executed epoch can answer into
    //! pre-formatted text, in place.
    void resolve_replies()
    {
        const Epoch& ep = epoch_;
        for (auto& [fd, conn] : conns_) {
            Conn& c = *conn;
            for (Reply& r : c.replies) {
                std::string text;
                switch (r.kind) {
                    case Reply::Kind::Text:
                        continue;
                    case Reply::Kind::Get: {
                        const value_int64_t v = ep.get_results[r.idx];
                        if (v == NOT_FOUND_VALUE)
                            text += NIL_BULK;
                        else
                            put_bulk_u64(text, static_cast<uint64_t>(v));
                        break;
                    }
                    case Reply::Kind::Exists: {
                        uint64_t nr_found = 0;
                        for (uint32_t i = 0; i < r.n; i++)
                            nr_found += ep.get_results[r.idx + i] != NOT_FOUND_VALUE;
                        put_int(text, nr_found);
                        break;
                    }
                    case Reply::Kind::Del: {
                        uint64_t nr_deleted = 0;
                        for (uint32_t i = 0; i < r.n; i++)
                            nr_deleted += ep.del_results[r.idx + i] != 0;
                        put_int(text, nr_deleted);
                        break;
                    }
                    case Reply::Kind::Pred: {
                        const KVPair p = ep.pred_results[r.idx];
                        if (p.value == NOT_FOUND_VALUE) {
                            text += NIL_ARRAY;  // no live strict predecessor
                            break;
                        }
                        text += "*2\r\n";
                        put_bulk_u64(text, p.key);
                        put_bulk_u64(text, static_cast<uint64_t>(p.value));
                        break;
                    }
                    case Reply::Kind::Count:
                        put_int(text, ep.count_results[r.idx]);
                        break;
                    case Reply::Kind::Max: {
                        const value_int64_t v = ep.max_results[r.idx];
                        if (v == NOT_FOUND_VALUE)
                            text += NIL_BULK;
                        else
                            put_bulk_u64(text, static_cast<uint64_t>(v));
                        break;
                    }
                    case Reply::Kind::DbSize: {
                        uint64_t total = 0;
                        for (const auto& per_dpu : forest_.get_nr_pairs())
                            total += per_dpu[0] + per_dpu[1];
                        put_int(text, total);
                        break;
                    }
                }
                r.kind = Reply::Kind::Text;
                r.text = std::move(text);
            }
            c.cls = CmdClass::None;
        }
    }

    //! Moves each connection's resolved replies into its output buffer in
    //! order.
    void emit_ready_replies()
    {
        for (auto& [fd, conn] : conns_) {
            Conn& c = *conn;
            while (!c.replies.empty() && c.replies.front().kind == Reply::Kind::Text) {
                c.out += c.replies.front().text;
                c.replies.pop_front();
                nr_pending_replies_--;
            }
        }
    }

    /* output flushing and connection teardown */

    void flush_all()
    {
        for (auto& [fd, conn] : conns_)
            if (!conn->dead)
                flush_conn(*conn);
    }

    void flush_conn(Conn& c)
    {
        while (c.out_off < c.out.size()) {
            const ssize_t w = write(c.fd, c.out.data() + c.out_off, c.out.size() - c.out_off);
            if (w > 0) {
                c.out_off += static_cast<size_t>(w);
            } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                set_epollout(c, true);
                return;
            } else if (w < 0 && errno == EINTR) {
                continue;
            } else {
                c.dead = true;
                return;
            }
        }
        c.out.clear();
        c.out_off = 0;
        set_epollout(c, false);
        if (c.close_after_flush || (c.read_eof && c.cmds.empty() && c.replies.empty()))
            c.dead = true;
    }

    void set_epollout(Conn& c, bool on)
    {
        if (c.epollout == on)
            return;
        epoll_event ev{};
        ev.events = EPOLLIN | (on ? EPOLLOUT : 0u);
        ev.data.fd = c.fd;
        epoll_ctl(epfd_, EPOLL_CTL_MOD, c.fd, &ev);
        c.epollout = on;
    }

    void reap_dead_conns()
    {
        for (auto it = conns_.begin(); it != conns_.end();) {
            if (it->second->dead) {
                nr_pending_replies_ -= it->second->replies.size();
                epoll_ctl(epfd_, EPOLL_CTL_DEL, it->first, nullptr);
                close(it->first);
                it = conns_.erase(it);
            } else {
                ++it;
            }
        }
    }

    BPForest& forest_;
    Options opt_;
    int listen_fd_ = -1;
    int epfd_ = -1;
    std::unordered_map<int, std::unique_ptr<Conn>> conns_;
    Epoch epoch_;
    size_t nr_epoch_queries_ = 0;
    size_t nr_pending_replies_ = 0;
    bool shutdown_cmd_ = false;
};


struct BPForestOption {
    void add_options(cmdline::parser& a)
    {
        a.add<unsigned>("balancing-param", 'a', "the tunable parameter (>= 1) for compute/memory load balancing in B+-Forest", false, 1,
            cmdline::range(1u, std::numeric_limits<unsigned>::max()));
        a.add<unsigned>("more-hot", 'h', "the tunable parameter for hotness of hot partitions", false, 1);
        a.add<bool>("greedy-only", 0, "whether to select hot ranges only with the greedy scan, skipping the argmax-window scan", false, false);
        a.add<bool>("dynamic-repartition", 0, "whether to adaptively repartition when overload is detected during batch execution", false, true);
        a.add<bool>("incremental", 0, "whether to enable incremental rebalancing", false, true);
        a.add<bool>("hot-split", 0, "whether to enable splitting an already-hot partition that re-overheats", false, true);
        add_overload_threshold_options(a);
        a.add<unsigned>("nr-host-threads", 't', "num of threads used in pre/post-processing in B+-Forest", false, 0);
    }
    void set_options(cmdline::parser& a)
    {
        param.balancing = a.get<unsigned>("balancing-param");
        param.more_hotness = a.get<unsigned>("more-hot");
        param.greedy_only = a.get<bool>("greedy-only");
        param.enable_dynamic_repartition = a.get<bool>("dynamic-repartition");
        param.enable_incremental = a.get<bool>("incremental");
        param.enable_hot_split = a.get<bool>("hot-split");
        param.nr_host_threads = a.get<unsigned>("nr-host-threads");
        param.overload_threshold_spec = parse_overload_threshold_spec(a).value_or(HighWatermarkRatio{1.05});
    }

    BPForest::Param param;
};

}  // namespace


int main(int argc, char* argv[])
{
    if (numa_available() >= 0) {
        if (numa_run_on_node(0) != 0) {
            throw std::system_error{errno, std::generic_category(), "numa_run_on_node"};
        }
    }

    cmdline::parser a;
    a.add<std::string>("bind", 'b', "address to listen on", false, "127.0.0.1");
    a.add<int>("port", 'p', "port to listen on", false, 6399, cmdline::range(1, 65535));
    a.add<size_t>("init-nr", 'n', "number of generated initial pairs (key = i * stride, value = the low bits of the key)", false, size_t{1} << 20);
    a.add<uint64_t>("init-stride", 0, "key stride of generated initial pairs", false, 2, cmdline::range<uint64_t>(1, uint64_t{1} << 32));
    a.add<std::optional<std::string>>("init-file", 'i', "file path to PIM-Tree init file to bulk-load instead of generated pairs", false);
    a.add<size_t>("batch-size", 0, "max queries executed in one batch (epoch)", false, size_t{1} << 20);
    a.add<size_t>("max-pipeline", 0, "max parsed-but-unexecuted commands per connection", false, size_t{1} << 20);
    BPForestOption forest_opt;
    forest_opt.add_options(a);
    a.parse_check(argc, argv);

    RespServer::Options sopt;
    sopt.bind_addr = a.get<std::string>("bind");
    sopt.port = a.get<int>("port");
    sopt.epoch_cap = a.get<size_t>("batch-size");
    sopt.max_pipeline = a.get<size_t>("max-pipeline");
    forest_opt.set_options(a);

    std::vector<KVPair> init_pairs;
    if (const std::optional<std::string> init_file = a.get<std::optional<std::string>>("init-file")) {
        InitData init_data{*init_file};
        init_pairs = init_data.get_data();
    } else {
        const size_t nr = a.get<size_t>("init-nr");
        const uint64_t stride = a.get<uint64_t>("init-stride");
        init_pairs.resize(nr);
        for (size_t i = 0; i < nr; i++) {
            const key_uint64_t key = (static_cast<key_uint64_t>(i) + 1) * stride;
            init_pairs[i] = KVPair{key, static_cast<value_int64_t>(key & static_cast<key_uint64_t>(VALUE_MAX))};
        }
    }
    if (init_pairs.empty()) {
        std::fprintf(stderr, "resp_server: initial data must not be empty (BPForest is bulk-load only)\n");
        return 1;
    }

    BPForest forest{init_pairs.data(), init_pairs.size(), forest_opt.param};

    struct sigaction sa{};
    sa.sa_handler = handle_stop_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);
    signal(SIGPIPE, SIG_IGN);

    RespServer server{forest, sopt};
    std::printf("resp_server: listening on %s:%d (%u DPUs, %zu initial pairs)\n",
        sopt.bind_addr.c_str(), sopt.port, upmem_get_nr_dpus(), init_pairs.size());
    std::fflush(stdout);
    server.run();
    std::printf("resp_server: shutting down\n");
    return 0;
}
