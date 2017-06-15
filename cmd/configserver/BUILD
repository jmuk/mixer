package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_binary(
    name = "configserver",
    srcs = [
        "main.go",
    ],
    linkstamp = "istio.io/mixer/pkg/version",
    visibility = ["//visibility:public"],
    deps = [
        "//cmd/server/cmd:go_default_library",
        "//cmd/shared:go_default_library",
        "//pkg/config:go_default_library",
        "//pkg/config/store:go_default_library",
        "//pkg/tracing:go_default_library",
        "@com_github_grpc_ecosystem_grpc_gateway//runtime:go_default_library",
        "@com_github_grpcecosystem_opentracing//go/otgrpc:go_default_library",
        "@com_github_istio_api//:config/v1",
        "@com_github_opentracing_basictracer//:go_default_library",
        "@com_github_opentracing_opentracing_go//:go_default_library",
        "@com_github_spf13_cobra//:go_default_library",
        "@org_golang_google_grpc//:go_default_library",
    ],
)
