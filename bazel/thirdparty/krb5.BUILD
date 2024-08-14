load("@rules_foreign_cc//foreign_cc:defs.bzl", "configure_make")

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

configure_make(
    name = "krb5",
    autoreconf = True,
    autoreconf_options = ["-ivf ./src"],
    configure_command = "./src/configure",
    configure_in_place = True,
    configure_options = [
        "--srcdir=./src",
        "--disable-thread-support",
        "--without-netlib",
        # TODO(bazel) when building the static library the linker is exiting with a
        # duplicate symbol error
        "--enable-shared",
        "--disable-static",
    ],
    lib_source = ":srcs",
    out_shared_libs = [
        "libkrb5.so",
        "libgssapi_krb5.so",
    ],
    visibility = [
        "//visibility:public",
    ],
)