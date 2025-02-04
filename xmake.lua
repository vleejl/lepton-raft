add_rules("mode.debug", "mode.release")
add_rules("plugin.compile_commands.autoupdate", {outputdir = ".vscode"})
set_languages("cxx2b")
set_warnings("all")
-- set_policy("build.sanitizer.address", true)
-- set_policy("build.sanitizer.thread", true)
-- set_policy("build.sanitizer.memory", true)
-- set_policy("build.sanitizer.undefined", true)
add_cxflags("-fno-permissive", {force = true})
add_cxflags("-std=c++20 -pedantic", {force = true})
add_cxflags("-Wconversion", {force = true})

add_requires("abseil")
add_requires("asio")
add_requires("benchmark")
add_requires("catch2")
add_requires("fmt", {configs = {header_only = false}})
add_requires("gflags")
add_requires("gtest")
add_requires("magic_enum")
add_requires("protoc", "protobuf-cpp")
add_requires("spdlog", {configs = {fmt_external = true, header_only = false}})

add_includedirs("third_party/leaf/")
add_includedirs("third_party/proxy/")
add_includedirs("include/")
add_includedirs("include/confchange")
add_includedirs("include/quorum")
add_includedirs("include/raftpb")
add_includedirs("include/tracker")

target("lepton-raft")
    set_kind("binary")
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft souce file
    add_files("src/confchange/*.cpp")
    add_files("src/tracker/*.cpp")
    add_files("src/*.cpp")
    add_packages("asio", "abseil", "fmt", "magic_enum", "spdlog")

target("lepton-unit-test")
    set_policy("build.sanitizer.leak", true)
    add_defines("LEPTON_TEST")
    add_defines("LEPTON_PROJECT_DIR=\"$(curdir)\"")
    add_includedirs("test/utility/include")
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft souce file
    add_files("src/confchange/*.cpp")
    add_files("src/tracker/*.cpp")
    add_files("src/*.cpp|main.cpp")
    -- lepton-raft unit test file
    add_files("test/unit_test.cpp")
    add_files("test/asio/*.cpp")
    add_files("test/confchange/*.cpp")
    add_files("test/quorum/*.cpp")
    add_files("test/raft/*.cpp")
    add_files("test/tracker/*.cpp")
    add_files("test/utility/src/*.cpp")
    add_packages("asio", "abseil", "fmt", "magic_enum", "spdlog")
    add_packages("catch2")
    add_packages("gtest", "benchmark")


target("lepton-benchmark-test")
    -- lepton-raft protobuf file
    add_rules("protobuf.cpp")
    add_files("proto/**.proto", {proto_rootdir = "proto"})
    add_packages("protoc", "protobuf-cpp")
    -- lepton-raft benchmark test file
    add_defines("LEPTON_TEST")
    add_files("test/benchmark.cpp")
    add_files("test/quorum/test_quorum_benchmark.cpp")
    add_packages("asio", "abseil", "fmt", "magic_enum", "spdlog")
    add_packages("gtest", "benchmark")    

-- 更新本地仓库 package 版本
-- xmake repo -u 