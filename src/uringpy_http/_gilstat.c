/*
 * uringpy_http._gilstat: how often the GIL changed owner.
 *
 * CPython counts, in the GIL itself, every acquisition by a thread other than
 * the one that held the lock last (`switch_number` in struct
 * _gil_runtime_state, incremented in take_gil() in Python/ceval_gil.c). That
 * is a hand-off of the lock from one thread to another, counted by the
 * interpreter, not inferred. Since Python 3.12 each interpreter has its own
 * GIL, reached through interp->ceval.gil.
 *
 * The struct is internal, so this module is built against the interpreter's
 * internal headers and must be compiled for the exact version it runs on
 * (which pip does). setup.py marks it optional: if those headers change, the
 * rest of the package still builds and switch_count() is simply unavailable.
 */
#define PY_SSIZE_T_CLEAN
#ifndef Py_BUILD_CORE_MODULE
#  define Py_BUILD_CORE_MODULE 1
#endif
#include <Python.h>

#if PY_VERSION_HEX >= 0x030C0000
#  include "internal/pycore_interp.h"
#  define HAVE_GIL_COUNTER 1
#endif

static PyObject *
switch_count(PyObject *self, PyObject *Py_UNUSED(ignored))
{
#ifdef HAVE_GIL_COUNTER
    PyInterpreterState *interp = PyInterpreterState_Get();
    struct _gil_runtime_state *gil = interp->ceval.gil;
    if (gil == NULL) {
        Py_RETURN_NONE;
    }
    /* A plain read of a counter that only grows; it is read once, at exit. */
    return PyLong_FromUnsignedLong(gil->switch_number);
#else
    Py_RETURN_NONE;
#endif
}

static PyMethodDef methods[] = {
    {"switch_count", switch_count, METH_NOARGS,
     "Number of times the GIL of this interpreter was taken by a thread other "
     "than its previous holder (CPython's own counter), or None where it "
     "cannot be read."},
    {NULL, NULL, 0, NULL},
};

static PyModuleDef_Slot slots[] = {
#ifdef Py_mod_multiple_interpreters
    {Py_mod_multiple_interpreters, Py_MOD_PER_INTERPRETER_GIL_SUPPORTED},
#endif
#ifdef Py_mod_gil
    {Py_mod_gil, Py_MOD_GIL_NOT_USED},
#endif
    {0, NULL},
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT, "_gilstat",
    "How often the GIL changed owner, from CPython's own counter.",
    0, methods, slots, NULL, NULL, NULL,
};

PyMODINIT_FUNC
PyInit__gilstat(void)
{
    return PyModuleDef_Init(&module);
}
