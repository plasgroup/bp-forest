#pragma once

#include <cstdlib>
#include <iostream>


#define ASSERT(expr) (static_cast<bool>(expr) ? void(0) : ((std::cerr << __FILE__ ":" << __LINE__ << ": Assertion `" #expr "' failed" << std::endl), std::abort()))
