workspace(name = "com_github_istio_mixer")

git_repository(
    name = "io_bazel_rules_go",
    commit = "4c9a52aba0b59511c5646af88d2f93a9c0193647",  # May 5, 2017 (0.4.4)
    remote = "https://github.com/bazelbuild/rules_go.git",
)

load("@io_bazel_rules_go//go:def.bzl", "go_repositories", "new_go_repository")

go_repositories()

git_repository(
    name = "io_bazel_rules_docker",
    commit = "e770f81cef4165828df955f37b827874a884a1de",  # June 21, 2017 (v0.0.2)
    remote = "https://github.com/bazelbuild/rules_docker.git",
)

load("@io_bazel_rules_docker//docker:docker.bzl", "docker_repositories")

docker_repositories()

git_repository(
    name = "org_pubref_rules_protobuf",
    commit = "9ede1dbc38f0b89ae6cd8e206a22dd93cc1d5637",  # Mar 31, 2017 (gogo* support)
    remote = "https://github.com/pubref/rules_protobuf",
)

load("@org_pubref_rules_protobuf//protobuf:rules.bzl", "proto_repositories")

proto_repositories()

load("@org_pubref_rules_protobuf//gogo:rules.bzl", "gogo_proto_repositories")
load("@org_pubref_rules_protobuf//cpp:rules.bzl", "cpp_proto_repositories")

cpp_proto_repositories()

gogo_proto_repositories()

new_go_repository(
    name = "com_github_golang_glog",
    commit = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",  # Jan 26, 2016 (no releases)
    importpath = "github.com/golang/glog",
)

new_go_repository(
    name = "com_github_ghodss_yaml",
    commit = "04f313413ffd65ce25f2541bfd2b2ceec5c0908c",  # Dec 6, 2016 (no releases)
    importpath = "github.com/ghodss/yaml",
)

new_go_repository(
    name = "in_gopkg_yaml_v2",
    commit = "14227de293ca979cf205cd88769fe71ed96a97e2",  # Jan 24, 2017 (no releases)
    importpath = "gopkg.in/yaml.v2",
)

new_go_repository(
    name = "com_github_golang_protobuf",
    commit = "8ee79997227bf9b34611aee7946ae64735e6fd93",  # Nov 16, 2016 (no releases)
    importpath = "github.com/golang/protobuf",
)

GOOGLEAPIS_BUILD_FILE = """
package(default_visibility = ["//visibility:public"])

load("@io_bazel_rules_go//go:def.bzl", "go_prefix")
go_prefix("github.com/googleapis/googleapis")

load("@org_pubref_rules_protobuf//gogo:rules.bzl", "gogoslick_proto_library")

gogoslick_proto_library(
    name = "google/rpc",
    protos = [
        "google/rpc/code.proto",
        "google/rpc/error_details.proto",
        "google/rpc/status.proto",
    ],
    importmap = {
        "google/protobuf/any.proto": "github.com/gogo/protobuf/types",
        "google/protobuf/duration.proto": "github.com/gogo/protobuf/types",
    },
    imports = [
        "../../external/com_github_google_protobuf/src",
    ],
    inputs = [
        "@com_github_google_protobuf//:well_known_protos",
    ],
    deps = [
        "@com_github_gogo_protobuf//types:go_default_library",
    ],
    verbose = 0,
)

load("@org_pubref_rules_protobuf//cpp:rules.bzl", "cc_proto_library")

cc_proto_library(
    name = "cc_status_proto",
    protos = [
        "google/rpc/status.proto",
    ],
    imports = [
        "../../external/com_github_google_protobuf/src",
    ],
    verbose = 0,
)

filegroup(
    name = "status_proto",
    srcs = [ "google/rpc/status.proto" ],
)

filegroup(
    name = "code_proto",
    srcs = [ "google/rpc/code.proto" ],
)
"""

new_git_repository(
    name = "com_github_googleapis_googleapis",
    build_file_content = GOOGLEAPIS_BUILD_FILE,
    commit = "13ac2436c5e3d568bd0e938f6ed58b77a48aba15",  # Oct 21, 2016 (only release pre-dates sha)
    remote = "https://github.com/googleapis/googleapis.git",
)

new_go_repository(
    name = "org_golang_google_grpc",
    commit = "8050b9cbc271307e5a716a9d782803d09b0d6f2d",  # Apr 7, 2017 (v1.2.1)
    importpath = "google.golang.org/grpc",
)

new_go_repository(
    name = "com_github_spf13_cobra",
    commit = "35136c09d8da66b901337c6e86fd8e88a1a255bd",  # Jan 30, 2017 (no releases)
    importpath = "github.com/spf13/cobra",
)

new_go_repository(
    name = "com_github_spf13_pflag",
    commit = "9ff6c6923cfffbcd502984b8e0c80539a94968b7",  # Jan 30, 2017 (no releases)
    importpath = "github.com/spf13/pflag",
)

new_go_repository(
    name = "com_github_cpuguy83_go_md2man",
    commit = "648eed146d3f3beacb64063cd0daae908015eebd",  # Mar 19, 2017 (no releases)
    importpath = "github.com/cpuguy83/go-md2man",
)

new_go_repository(
    name = "com_github_russross_blackfriday",
    commit = "35eb537633d9950afc8ae7bdf0edb6134584e9fc",  # Mar 19, 2017 (no releases)
    importpath = "github.com/russross/blackfriday",
)

new_go_repository(
    name = "com_github_shurcooL_sanitized_anchor_name",
    commit = "10ef21a441db47d8b13ebcc5fd2310f636973c77",  # Mar 19, 2017 (no releases)
    importpath = "github.com/shurcooL/sanitized_anchor_name",
)

new_go_repository(
    name = "com_github_hashicorp_go_multierror",
    commit = "ed905158d87462226a13fe39ddf685ea65f1c11f",  # Dec 16, 2016 (no releases)
    importpath = "github.com/hashicorp/go-multierror",
)

new_go_repository(
    name = "com_github_hashicorp_errwrap",
    commit = "7554cd9344cec97297fa6649b055a8c98c2a1e55",  # Oct 27, 2014 (no releases)
    importpath = "github.com/hashicorp/errwrap",
)

new_go_repository(
    name = "com_github_opentracing_opentracing_go",
    commit = "1949ddbfd147afd4d964a9f00b24eb291e0e7c38",  # May 3, 2017 (v1.0.2)
    importpath = "github.com/opentracing/opentracing-go",
)

new_go_repository(
    name = "com_github_opentracing_basictracer",
    commit = "1b32af207119a14b1b231d451df3ed04a72efebf",  # Sep 29, 2016 (no releases)
    importpath = "github.com/opentracing/basictracer-go",
)

load("//:repositories.bzl", "new_git_or_local_repository")

new_git_or_local_repository(
    name = "com_github_istio_api",
    build_file = "BUILD.api",
    commit = "ee9769f5b3304d9e01cd7ed6fb1dbb9b08e96210",  # June 29, 2017 (no releases)
    path = "../api",
    remote = "https://github.com/istio/api.git",
    # Change this to True to use ../api directory
    use_local = False,
)

new_http_archive(
    name = "docker_ubuntu",
    build_file = "BUILD.ubuntu",
    sha256 = "2c63dd81d714b825acd1cb3629c57d6ee733645479d0fcdf645203c2c35924c5",
    type = "zip",
    url = "https://codeload.github.com/tianon/docker-brew-ubuntu-core/zip/b6f1fe19228e5b6b7aed98dcba02f18088282f90",
)

DEBUG_BASE_IMAGE_SHA = "3f57ae2aceef79e4000fb07ec850bbf4bce811e6f81dc8cfd970e16cdf33e622"

# See github.com/istio/manager/blob/master/docker/debug/build-and-publish-debug-image.sh
# for instructions on how to re-build and publish this base image layer.
http_file(
    name = "ubuntu_xenial_debug",
    sha256 = DEBUG_BASE_IMAGE_SHA,
    url = "https://storage.googleapis.com/istio-build/manager/ubuntu_xenial_debug-" + DEBUG_BASE_IMAGE_SHA + ".tar.gz",
)

new_go_repository(
    name = "com_github_prometheus_client_golang",
    commit = "c5b7fccd204277076155f10851dad72b76a49317",  # Aug 17, 2016 (v0.8.0)
    importpath = "github.com/prometheus/client_golang",
)

new_go_repository(
    name = "com_github_prometheus_common",
    commit = "dd2f054febf4a6c00f2343686efb775948a8bff4",  # Jan 8, 2017 (no releases)
    importpath = "github.com/prometheus/common",
)

new_go_repository(
    name = "com_github_matttproud_golang_protobuf_extensions",
    commit = "c12348ce28de40eed0136aa2b644d0ee0650e56c",  # Apr 24, 2016 (v1.0.0)
    importpath = "github.com/matttproud/golang_protobuf_extensions",
)

new_go_repository(
    name = "com_github_prometheus_procfs",
    commit = "1878d9fbb537119d24b21ca07effd591627cd160",  # Jan 28, 2017 (no releases)
    importpath = "github.com/prometheus/procfs",
)

new_go_repository(
    name = "com_github_beorn7_perks",
    commit = "4c0e84591b9aa9e6dcfdf3e020114cd81f89d5f9",  # Aug 4, 2016 (no releases)
    importpath = "github.com/beorn7/perks",
)

new_go_repository(
    name = "com_github_prometheus_client_model",
    commit = "fa8ad6fec33561be4280a8f0514318c79d7f6cb6",  # Feb 12, 2015 (only release too old)
    importpath = "github.com/prometheus/client_model",
)

new_go_repository(
    name = "com_github_cactus_statsd_client",
    commit = "91c326c3f7bd20f0226d3d1c289dd9f8ce28d33d",  # release 3.1.0, 5/30/2016
    importpath = "github.com/cactus/go-statsd-client",
)

new_go_repository(
    name = "com_github_redis_client",
    commit = "1ac54a28f5934ea5e08f588647e734aba2383cb8",  # Jan 28, 2017 (no releases)
    importpath = "github.com/mediocregopher/radix.v2",
)

new_go_repository(
    name = "com_github_mini_redis",
    commit = "e9169f14d501184b6cc94e270e5a93e4bab203d7",  # release 2.0.0, 4/15/2017
    importpath = "github.com/alicebob/miniredis",
)

new_go_repository(
    name = "com_github_bsm_redeo",
    commit = "1ce09fc76693fb3c1ca9b529c66f38920beb6fb8",  # Aug 17, 2016 (no releases)
    importpath = "github.com/bsm/redeo",
)

new_go_repository(
    name = "io_k8s_apimachinery",
    commit = "20e10d54608f05c3059443a6c0afb9979641e88d",
    importpath = "k8s.io/apimachinery",
)

new_go_repository(
    name = "io_k8s_client_go",
    commit = "4e221f82e2ad6e61bd6190602de9c3400d79f1aa",  # Apr 4, 2017
    importpath = "k8s.io/client-go",
)

new_go_repository(
    name = "com_github_ugorji_go",
    commit = "708a42d246822952f38190a8d8c4e6b16a0e600c",  # Mar 12, 2017 (no releases)
    importpath = "github.com/ugorji/go",
)

new_go_repository(
    name = "in_gopkg_inf_v0",
    commit = "3887ee99ecf07df5b447e9b00d9c0b2adaa9f3e4",  # Sep 11, 2015 (latest commit)
    importpath = "gopkg.in/inf.v0",
)

new_go_repository(
    name = "com_github_docker_distribution",
    commit = "a25b9ef0c9fe242ac04bb20d3a028442b7d266b6",  # Apr 5, 2017 (v2.6.1)
    importpath = "github.com/docker/distribution",
)

new_go_repository(
    name = "com_github_go_openapi_spec",
    commit = "6aced65f8501fe1217321abf0749d354824ba2ff",  # Aug 8, 2016 (no releases)
    importpath = "github.com/go-openapi/spec",
)

new_go_repository(
    name = "com_github_google_gofuzz",
    commit = "44d81051d367757e1c7c6a5a86423ece9afcf63c",  # Nov 22, 2016 (no releases)
    importpath = "github.com/google/gofuzz",
)

new_go_repository(
    name = "com_github_emicklei_go_restful",
    commit = "09691a3b6378b740595c1002f40c34dd5f218a22",  # Dec 12, 2016 (k8s deps)
    importpath = "github.com/emicklei/go-restful",
)

new_go_repository(
    name = "com_github_go_openapi_jsonpointer",
    commit = "46af16f9f7b149af66e5d1bd010e3574dc06de98",  # Jul 4, 2016 (no releases)
    importpath = "github.com/go-openapi/jsonpointer",
)

new_go_repository(
    name = "com_github_go_openapi_jsonreference",
    commit = "13c6e3589ad90f49bd3e3bbe2c2cb3d7a4142272",  # Jul 4, 2016 (no releases)
    importpath = "github.com/go-openapi/jsonreference",
)

new_go_repository(
    name = "com_github_go_openapi_swag",
    commit = "1d0bd113de87027671077d3c71eb3ac5d7dbba72",  # Jul 4, 2016 (no releases)
    importpath = "github.com/go-openapi/swag",
)

new_go_repository(
    name = "org_golang_x_oauth2",
    commit = "3c3a985cb79f52a3190fbc056984415ca6763d01",  # Aug 26, 2016 (no releases)
    importpath = "golang.org/x/oauth2",
)

new_go_repository(
    name = "com_github_juju_ratelimit",
    commit = "acf38b000a03e4ab89e40f20f1e548f4e6ac7f72",  # Mar 13, 2017 (no releases)
    importpath = "github.com/juju/ratelimit",
)

new_go_repository(
    name = "com_github_opencontainers_go_digest",
    commit = "aa2ec055abd10d26d539eb630a92241b781ce4bc",  # Jan 31, 2017 (v1.0.0-rc0)
    importpath = "github.com/opencontainers/go-digest",
)

new_go_repository(
    name = "com_github_blang_semver",
    commit = "b38d23b8782a487059e8fc8773e9a5b228a77cb6",  # Jan 30, 2017 (v3.5.0)
    importpath = "github.com/blang/semver",
)

new_go_repository(
    name = "com_github_coreos_go_oidc",
    commit = "be73733bb8cc830d0205609b95d125215f8e9c70",  # Mar 7, 2017 (no releases)
    importpath = "github.com/coreos/go-oidc",
)

new_go_repository(
    name = "com_github_mailru_easyjson",
    commit = "2af9a745a611440bab0528e5ac19b2805a1c50eb",  # Mar 28, 2017 (no releases)
    importpath = "github.com/mailru/easyjson",
)

new_go_repository(
    name = "com_github_PuerkitoBio_purell",
    commit = "0bcb03f4b4d0a9428594752bd2a3b9aa0a9d4bd4",  # Nov 14, 2016 (v1.1.0)
    importpath = "github.com/PuerkitoBio/purell",
)

new_go_repository(
    name = "org_golang_x_text",
    build_file_name = "BUILD.bazel",
    commit = "f4b4367115ec2de254587813edaa901bc1c723a8",  # Mar 31, 2017 (no releases)
    importpath = "golang.org/x/text",
)

new_go_repository(
    name = "com_github_PuerkitoBio_urlesc",
    commit = "bbf7a2afc14f93e1e0a5c06df524fbd75e5031e5",  # Mar 24, 2017 (no releases)
    importpath = "github.com/PuerkitoBio/urlesc",
)

new_go_repository(
    name = "com_github_pborman_uuid",
    commit = "a97ce2ca70fa5a848076093f05e639a89ca34d06",  # Feb 9, 2016 (v1.0)
    importpath = "github.com/pborman/uuid",
)

new_go_repository(
    name = "com_google_cloud_go",
    commit = "a5913b3f7deecba45e98ff33cefbac4fd204ddd7",  # Jun 27, 2017 (v0.10.0)
    importpath = "cloud.google.com/go",
)

new_go_repository(
    name = "com_github_coreos_pkg",
    commit = "1c941d73110817a80b9fa6e14d5d2b00d977ce2a",  # Feb 6, 2017 (fix for build dir bazel issue)
    importpath = "github.com/coreos/pkg",
)

new_go_repository(
    name = "com_github_jonboulle_clockwork",
    commit = "2eee05ed794112d45db504eb05aa693efd2b8b09",  # Jul 6, 2016 (v0.1.0)
    importpath = "github.com/jonboulle/clockwork",
)

new_go_repository(
    name = "com_github_imdario_mergo",
    commit = "3e95a51e0639b4cf372f2ccf74c86749d747fbdc",  # Feb 16, 2016 (v0.2.2)
    importpath = "github.com/imdario/mergo",
)

new_go_repository(
    name = "com_github_howeyc_gopass",
    commit = "bf9dde6d0d2c004a008c27aaee91170c786f6db8",  # Jan 9, 2017 (no releases)
    importpath = "github.com/howeyc/gopass",
)

new_go_repository(
    name = "org_golang_x_crypto",
    commit = "cbc3d0884eac986df6e78a039b8792e869bff863",  # Apr 8, 2017 (no releases)
    importpath = "golang.org/x/crypto",
)

new_go_repository(
    name = "com_github_googleapis_gax_go",
    commit = "9af46dd5a1713e8b5cd71106287eba3cefdde50b",  # Mar 20, 2017 (no releases)
    importpath = "github.com/googleapis/gax-go",
)

new_go_repository(
    name = "com_github_hashicorp_golang_lru",
    commit = "0a025b7e63adc15a622f29b0b2c4c3848243bbf6",  # Aug 13, 2016 (no releases)
    importpath = "github.com/hashicorp/golang-lru",
)

new_go_repository(
    name = "com_github_grpcecosystem_opentracing",
    commit = "c94552f01d20ad74ec45a8cd967833a9d0b106cf",  # Feb 24, 2017 (no releases)
    importpath = "github.com/grpc-ecosystem/grpc-opentracing",
)

new_go_repository(
    name = "com_github_grpcecosystem_middleware",
    commit = "f63a7dfb64c138bd93d5c5b896d8b33c4b08e000",  # Jun 11, 2017 (no releases)
    importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
)

new_go_repository(
    name = "com_github_grpcecosystem_prometheus",
    commit = "2500245aa6110c562d17020fb31a2c133d737799",  # Mar 30, 2017 (only 1 release)
    importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
)

new_go_repository(
    name = "org_golang_google_api",
    commit = "1faa39f42f12a54fa82ca5902a7ab642d5b09ad1",  # Jun 5, 2017 (no releases)
    importpath = "google.golang.org/api",
)

new_go_repository(
    name = "org_golang_google_genproto",
    commit = "411e09b969b1170a9f0c467558eb4c4c110d9c77",  # Apr 4, 2017 (no release)
    importpath = "google.golang.org/genproto",
)

new_go_repository(
    name = "org_golang_x_tools",
    commit = "e6cb469339aef5b7be0c89de730d5f3cc8e47e50",  # Jun 23, 2017 (no releases)
    importpath = "golang.org/x/tools",
)

new_go_repository(
    name = "org_uber_go_zap",
    commit = "9cabc84638b70e564c3dab2766efcb1ded2aac9f",  # Jun 8, 2017 (v1.4.1)
    importpath = "go.uber.org/zap",
)

new_go_repository(
    name = "org_uber_go_atomic",
    commit = "4e336646b2ef9fc6e47be8e21594178f98e5ebcf",  # Apr 12, 2017 (v1.2.0)
    importpath = "go.uber.org/atomic",
)

# bazel rule for fixing up cfg.pb.go relies on running goimports
# we import it here as a git repository to allow projection of a
# simple build rule that will build the binary for usage (and avoid
# the need to project a more complicated BUILD file over the entire
# tools repo.)
new_git_repository(
    name = "org_golang_x_tools_imports",
    build_file = "BUILD.goimports",
    commit = "e6cb469339aef5b7be0c89de730d5f3cc8e47e50",  # Jun 23, 2017 (no releases)
    remote = "https://github.com/golang/tools.git",
)

new_go_repository(
    name = "com_github_Shopify_sarama",
    commit = "c01858abb625b73a3af51d0798e4ad42c8147093",  # May 8, 2017 (1.12.0)
    importpath = "github.com/Shopify/sarama",
)

new_go_repository(
    name = "com_github_apache_thrift",
    build_file_name = "BUILD.bazel",
    commit = "b2a4d4ae21c789b689dd162deb819665567f481c",  # Jan 6, 2017 (0.10.0)
    importpath = "github.com/apache/thrift",
)

new_go_repository(
    name = "com_github_go_logfmt_logfmt",
    commit = "390ab7935ee28ec6b286364bba9b4dd6410cb3d5",  # Nov 15, 2016 (0.3.0)
    importpath = "github.com/go-logfmt/logfmt",
)

new_go_repository(
    name = "com_github_eapache_queue",
    commit = "ded5959c0d4e360646dc9e9908cff48666781367",  # June 6, 2017 (1.0.2)
    importpath = "github.com/eapache/queue",
)

new_go_repository(
    name = "com_github_eapache_go_resiliency",
    commit = "6800482f2c813e689c88b7ed3282262385011890",  # Feb 13, 2015 (1.0.0)
    importpath = "github.com/eapache/go-resiliency",
)

new_go_repository(
    name = "com_github_eapache_go_xerial_snappy",
    commit = "bb955e01b9346ac19dc29eb16586c90ded99a98c",  # June 9, 2016 (no releases)
    importpath = "github.com/eapache/go-xerial-snappy",
)

new_go_repository(
    name = "com_github_rcrowley_go_metrics",
    commit = "1f30fe9094a513ce4c700b9a54458bbb0c96996c",  # Nov 28, 2016 (no releases)
    importpath = "github.com/rcrowley/go-metrics",
)

new_go_repository(
    name = "com_github_davecgh_go_spew",
    commit = "346938d642f2ec3594ed81d874461961cd0faa76",  # Nov 14, 2016 (1.1.0)
    importpath = "github.com/davecgh/go-spew",
)

new_go_repository(
    name = "com_github_pierrec_lz4",
    commit = "88df27974e3644957507a1ca1866edc8e98d4897",  # May 11, 2017 (no releases)
    importpath = "github.com/pierrec/lz4",
)

new_go_repository(
    name = "com_github_pierrec_xxHash",
    commit = "f051bb7f1d1aaf1b5a665d74fb6b0217712c69f7",  # March 20, 2016 (0.1.1)
    importpath = "github.com/pierrec/xxHash",
)

new_go_repository(
    name = "com_github_golang_snappy",
    commit = "553a641470496b2327abcac10b36396bd98e45c9",  # Feb 15, 2017 (no releases)
    importpath = "github.com/golang/snappy",
)

new_go_repository(
    name = "com_github_opentracing_contrib_go_observer",
    commit = "a52f2342449246d5bcc273e65cbdcfa5f7d6c63c",  # June 20, 2017 (no release)
    importpath = "github.com/opentracing-contrib/go-observer",
)

new_go_repository(
    name = "com_github_pmezard_go_difflib",
    commit = "792786c7400a136282c1664665ae0a8db921c6c2",  # August 8, 2016 (1.0.0)
    importpath = "github.com/pmezard/go-difflib",
)

new_go_repository(
    name = "com_github_stretchr_testify",
    commit = "69483b4bd14f5845b5a1e55bca19e954e827f1d0",  # September 24, 2016 (1.1.4)
    importpath = "github.com/stretchr/testify",
)

new_go_repository(
    name = "com_github_openzipkin_zipkin_go_opentracing",
    commit = "90d57f421daae5e385ce2429580f0d695c41823b",  # Jul 5, 2017 (has releases but we need a newer commit)
    importpath = "github.com/openzipkin/zipkin-go-opentracing",
)

##
## Docker image build deps
##

git_repository(
    name = "distroless",
    commit = "3af69e6d50747bca265e9699fe7cc0c80f6ed1e3",  # Jun 27, 2017 (no releases)
    remote = "https://github.com/GoogleCloudPlatform/distroless.git",
)

git_repository(
    name = "runtimes_common",
    commit = "3d73b4fecbd18de77588ab5eef712d50f34f601e",  # Jun 27, 2017 (no releases)
    remote = "https://github.com/GoogleCloudPlatform/runtimes-common.git",
)

load(
    "@distroless//package_manager:package_manager.bzl",
    "package_manager_repositories",
    "dpkg_src",
    "dpkg",
)

package_manager_repositories()

dpkg_src(
    name = "debian_jessie",
    arch = "amd64",
    distro = "jessie",
    url = "http://deb.debian.org",
)

dpkg_src(
    name = "debian_jessie_backports",
    arch = "amd64",
    distro = "jessie-backports",
    url = "http://deb.debian.org",
)

# For the glibc base image.
dpkg(
    name = "libc6",
    source = "@debian_jessie//file:Packages.json",
)

dpkg(
    name = "ca-certificates",
    source = "@debian_jessie//file:Packages.json",
)

dpkg(
    name = "openssl",
    source = "@debian_jessie//file:Packages.json",
)

dpkg(
    name = "libssl1.0.0",
    source = "@debian_jessie//file:Packages.json",
)

##
## Testing
##

git_repository(
    name = "com_github_istio_test_infra",
    commit = "9a3ac467ba862432c75e42cecff7aa5c2980e3b8",  # Jun 18, 2017 (no releases)
    remote = "https://github.com/istio/test-infra.git",
)
