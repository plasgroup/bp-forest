# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.13

# Default target executed when no arguments are given to make.
default_target: all

.PHONY : default_target

# Allow only one "make -f Makefile2" at a time, but pass parallelism.
.NOTPARALLEL:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/upmem0054/hokuda/bp-forest-exp/bp-forest

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/upmem0054/hokuda/bp-forest-exp/bp-forest

#=============================================================================
# Targets provided globally by CMake.

# Special rule for the target rebuild_cache
rebuild_cache:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "Running CMake to regenerate build system..."
	/usr/bin/cmake -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR)
.PHONY : rebuild_cache

# Special rule for the target rebuild_cache
rebuild_cache/fast: rebuild_cache

.PHONY : rebuild_cache/fast

# Special rule for the target edit_cache
edit_cache:
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --cyan "No interactive CMake dialog available..."
	/usr/bin/cmake -E echo No\ interactive\ CMake\ dialog\ available.
.PHONY : edit_cache

# Special rule for the target edit_cache
edit_cache/fast: edit_cache

.PHONY : edit_cache/fast

# The main all target
all: cmake_check_build_system
	$(CMAKE_COMMAND) -E cmake_progress_start /home/upmem0054/hokuda/bp-forest-exp/bp-forest/CMakeFiles /home/upmem0054/hokuda/bp-forest-exp/bp-forest/CMakeFiles/progress.marks
	$(MAKE) -f CMakeFiles/Makefile2 all
	$(CMAKE_COMMAND) -E cmake_progress_start /home/upmem0054/hokuda/bp-forest-exp/bp-forest/CMakeFiles 0
.PHONY : all

# The main clean target
clean:
	$(MAKE) -f CMakeFiles/Makefile2 clean
.PHONY : clean

# The main clean target
clean/fast: clean

.PHONY : clean/fast

# Prepare targets for installation.
preinstall: all
	$(MAKE) -f CMakeFiles/Makefile2 preinstall
.PHONY : preinstall

# Prepare targets for installation.
preinstall/fast:
	$(MAKE) -f CMakeFiles/Makefile2 preinstall
.PHONY : preinstall/fast

# clear depends
depend:
	$(CMAKE_COMMAND) -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR) --check-build-system CMakeFiles/Makefile.cmake 1
.PHONY : depend

#=============================================================================
# Target rules for targets named dpu_program

# Build rule for target.
dpu_program: cmake_check_build_system
	$(MAKE) -f CMakeFiles/Makefile2 dpu_program
.PHONY : dpu_program

# fast build rule for target.
dpu_program/fast:
	$(MAKE) -f dpu/CMakeFiles/dpu_program.dir/build.make dpu/CMakeFiles/dpu_program.dir/build
.PHONY : dpu_program/fast

#=============================================================================
# Target rules for targets named dpu_program_redundant

# Build rule for target.
dpu_program_redundant: cmake_check_build_system
	$(MAKE) -f CMakeFiles/Makefile2 dpu_program_redundant
.PHONY : dpu_program_redundant

# fast build rule for target.
dpu_program_redundant/fast:
	$(MAKE) -f dpu/CMakeFiles/dpu_program_redundant.dir/build.make dpu/CMakeFiles/dpu_program_redundant.dir/build
.PHONY : dpu_program_redundant/fast

#=============================================================================
# Target rules for targets named host_app

# Build rule for target.
host_app: cmake_check_build_system
	$(MAKE) -f CMakeFiles/Makefile2 host_app
.PHONY : host_app

# fast build rule for target.
host_app/fast:
	$(MAKE) -f host/CMakeFiles/host_app.dir/build.make host/CMakeFiles/host_app.dir/build
.PHONY : host_app/fast

#=============================================================================
# Target rules for targets named workload_gen

# Build rule for target.
workload_gen: cmake_check_build_system
	$(MAKE) -f CMakeFiles/Makefile2 workload_gen
.PHONY : workload_gen

# fast build rule for target.
workload_gen/fast:
	$(MAKE) -f workload_gen/CMakeFiles/workload_gen.dir/build.make workload_gen/CMakeFiles/workload_gen.dir/build
.PHONY : workload_gen/fast

# Help Target
help:
	@echo "The following are some of the valid targets for this Makefile:"
	@echo "... all (the default if no target is provided)"
	@echo "... clean"
	@echo "... depend"
	@echo "... rebuild_cache"
	@echo "... edit_cache"
	@echo "... dpu_program"
	@echo "... dpu_program_redundant"
	@echo "... host_app"
	@echo "... workload_gen"
.PHONY : help



#=============================================================================
# Special targets to cleanup operation of make.

# Special rule to run CMake to check the build system integrity.
# No rule that depends on this can have commands that come from listfiles
# because they might be regenerated.
cmake_check_build_system:
	$(CMAKE_COMMAND) -S$(CMAKE_SOURCE_DIR) -B$(CMAKE_BINARY_DIR) --check-build-system CMakeFiles/Makefile.cmake 0
.PHONY : cmake_check_build_system

