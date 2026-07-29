#pragma once

#include "timer_tree.hpp"


#ifdef SYNCHRONOUS_DPU_EXEC
inline TimerTree Timer{
    {"init", {
                 {"table"},
                 {"send"},
                 {"exec"},
             }},
    {"partition", {
                      {"full_reb", {
                                       {"ret_all", {
                                                       {"command"},
                                                       {"exec"},
                                                       {"recv_npairs"},
                                                       {"alloc"},
                                                       {"recv"},
                                                   }},
                                       {"route"},
                                       {"find_hot"},
                                       {"table"},
                                       {"re", {{"route"}}},
                                       {"send"},
                                       {"exec"},
                                   }},
                  }},
    {"batch", {
                  {"route"},
                  {"full_reb", {
                                   {"ret_all", {
                                                   {"command"},
                                                   {"exec"},
                                                   {"recv_npairs"},
                                                   {"alloc"},
                                                   {"recv"},
                                               }},
                                   {"route"},
                                   {"find_hot"},
                                   {"table"},
                                   {"re", {{"route"}}},
                                   {"send"},
                                   {"exec"},
                               }},
                  {"inc_reb", {
                                  {"retrieve", {{"command"}, {"exec"}, {"recv_npairs"}, {"alloc"}, {"recv"}}},
                                  {"cold"},
                                  {"hot"},
                                  {"table"},
                                  {"send"},
                                  {"exec"},
                                  {"re", {{"route"}}},
                              }},
                  {"send"},
                  {"exec"},
                  {"recv"},
                  {"postproc"},
              }}};
#else
inline TimerTree Timer{
    {"init", {
                 {"table"},
                 {"send_exec"},
             }},
    {"partition", {
                      {"full_reb", {
                                       {"ret_all", {
                                                       {"command_exec_recv_npairs"},
                                                       {"alloc"},
                                                       {"recv"},
                                                   }},
                                       {"route"},
                                       {"find_hot"},
                                       {"table"},
                                       {"re", {{"route"}}},
                                       {"send_exec"},
                                   }},
                  }},
    {"batch", {
                  {"route"},
                  {"full_reb", {
                                   {"ret_all", {
                                                   {"command_exec_recv_npairs"},
                                                   {"alloc"},
                                                   {"recv"},
                                               }},
                                   {"route"},
                                   {"find_hot"},
                                   {"table"},
                                   {"re", {{"route"}}},
                                   {"send_exec"},
                               }},
                  {"inc_reb", {
                                  {"retrieve", {{"command_exec_recv_npairs"}, {"alloc"}, {"recv"}}},
                                  {"cold"},
                                  {"hot"},
                                  {"table"},
                                  {"send_exec"},
                                  {"re", {{"route"}}},
                              }},
                  {"send_exec_recv"},
                  {"postproc"},
              }}};
#endif
