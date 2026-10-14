"""Build uringpy-http.

liburing: a system copy is used when pkg-config finds one (liburing >= 2.2);
otherwise the copy in vendor/liburing (2.6) is configured and built as a
static library first, so `pip install` needs no system headers. Set
URINGPY_HTTP_VENDORED_LIBURING=1 to force the vendored copy.
"""
import os
import shutil
import subprocess
import sys

from setuptools import Extension, setup
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize

HERE = os.path.dirname(os.path.abspath(__file__))
VENDOR_URING = os.path.join(HERE, "vendor", "liburing")
PICO = os.path.join(HERE, "vendor", "picohttpparser")


def system_liburing():
    if os.environ.get("URINGPY_HTTP_VENDORED_LIBURING") == "1":
        return None
    if shutil.which("pkg-config") is None:
        return None
    try:
        subprocess.check_call(["pkg-config", "--atleast-version=2.2", "liburing"],
                              stderr=subprocess.DEVNULL)
        cflags = subprocess.check_output(["pkg-config", "--cflags", "liburing"]).decode().split()
        libs = subprocess.check_output(["pkg-config", "--libs", "liburing"]).decode().split()
        return cflags, libs
    except (subprocess.CalledProcessError, OSError):
        return None


class BuildExt(build_ext):
    def build_extensions(self):
        found = system_liburing()
        core = next(e for e in self.extensions if e.name == "uringpy_http._core")
        if found:
            cflags, libs = found
            core.include_dirs += [f[2:] for f in cflags if f.startswith("-I")]
            core.libraries += [l[2:] for l in libs if l.startswith("-l")]
            core.library_dirs += [l[2:] for l in libs if l.startswith("-L")]
        else:
            build_dir = os.path.join(os.path.abspath(self.build_temp), "liburing")
            if os.path.exists(build_dir):
                shutil.rmtree(build_dir)
            shutil.copytree(VENDOR_URING, build_dir)
            subprocess.check_call(["sh", "./configure", "--use-libc"], cwd=build_dir,
                                  stdout=subprocess.DEVNULL)
            subprocess.check_call(["make", "-C", "src", "liburing.a",
                                   "-j%d" % (os.cpu_count() or 2)], cwd=build_dir,
                                  stdout=subprocess.DEVNULL)
            core.include_dirs.append(os.path.join(build_dir, "src", "include"))
            core.extra_objects.append(os.path.join(build_dir, "src", "liburing.a"))
        super().build_extensions()


march = [] if os.environ.get("URINGPY_HTTP_PORTABLE") == "1" else ["-march=native"]

extensions = [
    Extension(
        "uringpy_http._core",
        sources=["src/uringpy_http/_core.pyx", os.path.join("vendor", "picohttpparser", "picohttpparser.c")],
        include_dirs=[PICO],
        extra_compile_args=["-O3"] + march,
    ),
    # CPython's own count of GIL owner changes (optional: needs internal headers).
    Extension(
        "uringpy_http._gilstat",
        sources=["src/uringpy_http/_gilstat.c"],
        optional=True,
    ),
]

setup(
    ext_modules=cythonize(extensions, compiler_directives={"language_level": "3"}),
    cmdclass={"build_ext": BuildExt},
)
