load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

config_setting(
    name = "debug_mode",
    values = {"compilation_mode": "dbg"},
)

config_setting(
    name = "release_mode",
    values = {"compilation_mode": "opt"},
)

configure_make(
    name = "openssl",
    configure_command = "Configure",
    configure_options = [
        "--libdir=lib",
    ] + select({
        ":debug_mode": ["--debug"],
        ":release_mode": ["--release"],
        "//conditions:default": [],
    }),
    lib_source = ":srcs",
    out_shared_libs = [
        "libssl.so",
        "libcrypto.so",
    ],
    visibility = [
        "//visibility:public",
    ],
)
