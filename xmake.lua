add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = ".vscode"})
set_languages("cxx23")
set_warnings("all", "extra")
set_policy("build.warning", true)

if is_plat("linux", "macosx") then
    add_cxflags("-fno-permissive", "-std=c++20", "-pedantic", "-Wall", "-Wextra", "-Wconversion", {force = true})
    add_cxflags("-fno-omit-frame-pointer")
    if is_plat("macosx") then
        add_cxflags("-fexperimental-library")
    end
    if is_kind("clang", "clangxx") then
        add_cxflags("-fsanitize-address-use-after-return=always")
        add_cxflags("-fsanitize-address-use-odr-indicator")
    elseif is_kind("gcc", "gxx") then
        add_cxflags("-fsanitize-address-use-after-scope")
        add_cxflags("-static-libasan")
    end
end

-- ========== 定义编译选项 ==========
option("asan")
    set_showmenu(true)
    set_default(false)
    add_defines("USE_ASAN")
option_end()

option("tsan")
    set_showmenu(true)
    set_default(false)
    add_defines("USE_TSAN")
option_end()

option("msan")
    set_showmenu(true)
    set_default(false)
    add_defines("USE_MSAN")
option_end()

-- ========== 通用函数：应用 Sanitizer ==========
function apply_sanitizers(target)
    if has_config("asan") then
        target:add("cxflags", "-fsanitize=address", "-fsanitize=undefined", "-fno-omit-frame-pointer", {force = true})
        target:add("ldflags", "-fsanitize=address", "-fsanitize=undefined")
    end
    if has_config("tsan") then
        target:add("cxflags", "-fsanitize=thread", "-fno-omit-frame-pointer", {force = true})
        target:add("ldflags", "-fsanitize=thread")
    end
    if has_config("msan") then
        target:add("cxflags", "-fsanitize=memory", "-fno-omit-frame-pointer")
        target:add("ldflags", "-fsanitize=memory")
    end
end

if is_plat("linux") then
    add_requires("liburing")
    add_defines("ASIO_HAS_FILE", "ASIO_HAS_IO_URING", "ASIO_HAS_LINUX_NATIVE_FILE_HANDLE")
end

add_requires("abseil")
add_requires("asio")
add_requires("benchmark")
add_requires("fmt", {configs = {header_only = false}})
add_requires("gtest")
add_requires("magic_enum")
add_requires("nlohmann_json")
add_requires("protoc", "protobuf-cpp")
add_requires("rocksdb", {configs = {rtti = true}})
add_requires("spdlog", {configs = {fmt_external = true, header_only = false}})
add_requires("tl_expected")

add_includedirs("include/basic")
add_includedirs("third_party/leaf/")
add_includedirs("third_party/proxy/include/proxy")

local test_cxflags
if is_plat("windows") then
    test_cxflags = {}
else
    test_cxflags = {
        "-Wno-unused-result",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
        "-Wno-missing-field-initializers"
    }
end

target("lepton-raft-core-unit-test")
    -- lepton basic include dirs
    add_includedirs("include/basic")
    add_includedirs("include/error")
    -- raft core include dirs 
    add_includedirs("include/raft_core")
    add_includedirs("include/raft_core/confchange")
    add_includedirs("include/raft_core/quorum")
    add_includedirs("include/raft_core/pb")
    add_includedirs("include/raft_core/tracker")
    -- raft core include dirs 
    add_includedirs("third_party/dtl")
    on_load(apply_sanitizers)
    add_defines("LEPTON_TEST")
    local project_dir = os.projectdir():gsub("\\", "/")
    add_defines("LEPTON_PROJECT_DIR=\"" .. project_dir .."\"")
    add_includedirs("test/raft_core/rafttest/include")
    add_includedirs("test/raft_core/utility/include")
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
    add_defines("SPDLOG_FORCE_COLOR")  -- 强制彩色输出
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft source file
    add_files("src/raft_core/confchange/*.cpp")
    add_files("src/raft_core/pb/*.cpp")
    add_files("src/raft_core/tracker/*.cpp")
    add_files("src/raft_core/*.cpp|main.cpp")
    -- lepton-raft basic utility unit test file
    add_files("test/raft_core/utility/src/*.cpp", {cxflags = test_cxflags})
    -- lepton-raft unit test file
    add_files("test/raft_core/unit_test.cpp", {cxflags = test_cxflags})
    add_files("test/asio/*.cpp", {cxflags = test_cxflags})
    add_files("test/raft_core/confchange/*.cpp", {cxflags = test_cxflags})
    add_files("test/raft_core/quorum/*.cpp", {cxflags = test_cxflags})
    add_files("test/raft_core/raft/*.cpp", {cxflags = test_cxflags})
    add_files("test/raft_core/rafttest/test/*.cpp", {cxflags = test_cxflags})
    add_files("test/spdlog/*.cpp", {cxflags = test_cxflags})
    add_files("test/third_party/*.cpp", {cxflags = test_cxflags})
    add_files("test/raft_core/tracker/*.cpp", {cxflags = test_cxflags})
    if is_plat("linux") then
        add_packages("liburing")
    end
    add_packages("asio", "abseil", "fmt", "magic_enum", "nlohmann_json", "rocksdb", "spdlog", "tl_expected")
    add_packages("gtest", "benchmark")


target("lepton-raft-core-benchmark-test")
    -- lepton basic include dirs
    add_includedirs("include/basic")
    add_includedirs("include/error")
    -- raft core include dirs 
    add_includedirs("include/raft_core")
    add_includedirs("include/raft_core/confchange")
    add_includedirs("include/raft_core/quorum")
    add_includedirs("include/raft_core/pb")
    add_includedirs("include/raft_core/tracker")
    -- raft core include dirs 
    add_includedirs("third_party/dtl")
    add_defines("LEPTON_TEST")
    add_defines("LEPTON_PROJECT_DIR=\"$(curdir)\"")
    add_includedirs("test/raft_core/utility/include")
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
    add_defines("SPDLOG_FORCE_COLOR")  -- 强制彩色输出
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft source file
    add_files("src/raft_core/confchange/*.cpp")
    add_files("src/raft_core/pb/*.cpp")
    add_files("src/raft_core/tracker/*.cpp")
    add_files("src/raft_core/*.cpp|main.cpp")
    -- lepton-raft basic utility unit test file
    add_files("test/raft_core/utility/src/*.cpp", {cxflags = test_cxflags})    
    -- lepton-raft benchmark test file
    add_files("test/raft_core/benchmark.cpp")
    add_files("test/raft_core/quorum/test_quorum_benchmark.cpp", {cxflags = test_cxflags})
    add_files("test/raft_core/raft/test_raw_node_benchmark.cpp", {cxflags = test_cxflags})
    if is_plat("linux") then
        add_packages("liburing")
    end    
    add_packages("asio", "abseil", "fmt", "magic_enum", "nlohmann_json", "rocksdb", "spdlog", "tl_expected")
    add_packages("gtest", "benchmark")

target("lepton-raft-storage-unit-test")
    -- lepton basic include dirs
    add_includedirs("include/basic")
    add_includedirs("include/error")
    -- raft core include dirs 
    add_includedirs("include/raft_core")
    add_includedirs("include/raft_core/confchange")
    add_includedirs("include/raft_core/quorum")
    add_includedirs("include/raft_core/pb")
    add_includedirs("include/raft_core/tracker")
    -- raft storage include dirs 
    add_includedirs("include/storage/fileutil")
    add_includedirs("include/storage/ioutil")
    add_includedirs("include/storage/pb")
    add_includedirs("include/storage/wal")
    on_load(apply_sanitizers)
    add_defines("LEPTON_TEST")
    add_defines("LEPTON_STORAGE")
    local project_dir = os.projectdir():gsub("\\", "/")
    add_defines("LEPTON_PROJECT_DIR=\"" .. project_dir .."\"")
    add_includedirs("test/raft_core/rafttest/include")
    add_includedirs("test/raft_core/utility/include")
    add_includedirs("test/storage/utility/include")
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
    add_defines("SPDLOG_FORCE_COLOR")  -- 强制彩色输出
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft source file
    add_files("src/raft_core/confchange/*.cpp")
    add_files("src/raft_core/pb/*.cpp")
    add_files("src/raft_core/tracker/*.cpp")
    add_files("src/raft_core/*.cpp|main.cpp")
    -- raft storage source dirs 
    add_files("src/storage/fileutil/*.cpp")
    add_files("src/storage/ioutil/*.cpp")
    add_files("src/storage/pb/*.cpp")
    add_files("src/storage/wal/*.cpp")  
    -- lepton-raft unit test file
    add_files("test/storage/utility/src/*.cpp", {cxflags = test_cxflags})
    add_files("test/storage/asio/*.cpp", {cxflags = test_cxflags})
    add_files("test/storage/rocksdb/*.cpp", {cxflags = test_cxflags})
    add_files("test/storage/ioutil/*.cpp", {cxflags = test_cxflags})
    -- lepton-raft benchmark test file
    add_files("test/storage/unit_test.cpp")
    if is_plat("linux") then
        add_packages("liburing")
    end    
    add_packages("asio", "abseil", "fmt", "magic_enum", "nlohmann_json", "poco", "rocksdb", "spdlog", "tl_expected")
    add_packages("gtest", "benchmark")

-- 更新本地仓库 package 版本
-- xmake repo -u 