add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = ".vscode"})
set_languages("cxx2b")
set_warnings("all", "extra")
set_policy("build.warning", true)

-- 基础内存检测 (GCC/Clang 通用)
set_policy("build.sanitizer.address", true)
set_policy("build.sanitizer.undefined", true)
set_policy("build.sanitizer.leak", true)
-- 编译器差异化配置
if is_plat("linux", "macosx") then
    -- 公共选项
    add_cxflags("-fno-omit-frame-pointer")
    
    -- Clang 专属增强检测
    if is_kind("clang", "clangxx") then
        add_cxflags("-fsanitize-address-use-after-return=always")
        add_cxflags("-fsanitize-address-use-odr-indicator")
    end
    
    -- GCC 替代方案
    if is_kind("gcc", "gxx") then
        add_cxflags("-fsanitize-address-use-after-scope")
        add_cxflags("-static-libasan")  -- 确保链接 ASan 库
    end
end
-- set_policy("build.sanitizer.address", true)
    -- set_policy("build.sanitizer.thread", true)
    -- set_policy("build.sanitizer.memory", true)
-- set_policy("build.sanitizer.leak", true)
-- set_policy("build.sanitizer.undefined", true)
add_cxflags("-fno-permissive", {force = true})
add_cxflags("-std=c++20 -pedantic", {force = true})
add_cxflags("-Wconversion", {force = true})

add_requires("abseil")
add_requires("asio")
add_requires("benchmark")
add_requires("fmt", {configs = {header_only = false}})
add_requires("gflags")
add_requires("gtest")
add_requires("magic_enum")
add_requires("nlohmann_json")
add_requires("protoc", "protobuf-cpp")
add_requires("spdlog", {configs = {fmt_external = true, header_only = false}})

add_includedirs("third_party/leaf/")
add_includedirs("third_party/proxy/")
add_includedirs("include/")
add_includedirs("include/basic")
add_includedirs("include/confchange")
add_includedirs("include/error")
add_includedirs("include/quorum")
add_includedirs("include/pb")
add_includedirs("include/tracker")

target("lepton-raft")
    set_kind("binary")
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft souce file
    add_files("src/confchange/*.cpp")
    add_files("src/pb/*.cpp")
    add_files("src/tracker/*.cpp")
    add_files("src/*.cpp")
    add_packages("asio", "abseil", "fmt", "magic_enum", "nlohmann_json", "spdlog")

target("lepton-unit-test")
    set_policy("build.sanitizer.leak", true)
    add_defines("LEPTON_TEST")
    add_defines("LEPTON_PROJECT_DIR=\"$(curdir)\"")
    add_includedirs("test/utility/include")
    add_defines("SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_DEBUG")
    add_defines("SPDLOG_FORCE_COLOR")  -- 强制彩色输出
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft souce file
    add_files("src/confchange/*.cpp")
    add_files("src/pb/*.cpp")
    add_files("src/tracker/*.cpp")
    add_files("src/*.cpp|main.cpp")
    -- lepton-raft unit test file
    add_files("test/unit_test.cpp")
    add_files("test/asio/*.cpp")
    add_files("test/confchange/*.cpp")
    add_files("test/quorum/*.cpp")
    add_files("test/raft/*.cpp")
    add_files("test/third_party/*.cpp")
    add_files("test/tracker/*.cpp")
    add_files("test/utility/src/*.cpp")
    add_packages("asio", "abseil", "fmt", "magic_enum", "nlohmann_json", "spdlog")
    add_packages("gtest", "benchmark")


target("lepton-benchmark-test")
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft souce file
    add_files("src/confchange/*.cpp")
    add_files("src/pb/*.cpp")
    add_files("src/tracker/*.cpp")
    add_files("src/*.cpp|main.cpp")
    -- lepton-raft benchmark test file
    add_defines("LEPTON_TEST")
    add_files("test/benchmark.cpp")
    add_files("test/quorum/test_quorum_benchmark.cpp")
    add_packages("asio", "abseil", "fmt", "magic_enum", "nlohmann_json", "spdlog")
    add_packages("gtest", "benchmark")    

-- 更新本地仓库 package 版本
-- xmake repo -u 