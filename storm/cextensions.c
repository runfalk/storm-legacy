/*
#
# Copyright (c) 2006, 2007 Canonical
#
# Written by Gustavo Niemeyer <gustavo@niemeyer.net>
#
# This file is part of Storm Object Relational Mapper.
#
# Storm is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; either version 2.1 of
# the License, or (at your option) any later version.
#
# Storm is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
*/
#include <Python.h>
#include <structmember.h>


static PyObject *Undef = NULL;
static PyObject *LazyValue = NULL;
static PyObject *raise_none_error = NULL;
static PyObject *get_cls_info = NULL;
static PyObject *EventSystem = NULL;


typedef struct {
    PyObject_HEAD
    PyObject *_value;
    PyObject *_lazy_value;
    PyObject *_checkpoint_state;
    PyObject *_allow_none;
    PyObject *column;
    PyObject *event;
} VariableObject;

typedef struct {
    PyDictObject super;
    PyObject *__weakreflist;
    PyObject *__obj_ref;
    PyObject *__obj_ref_callback;
    PyObject *cls_info;
    PyObject *event;
    PyObject *variables;
    PyObject *primary_vars;
} ObjectInfoObject;


static int
initialize_globals(void)
{
    static int initialized = 0;
    PyObject *module;
    
    if (initialized)
        return 1;

    initialized = 1;

    {
        module = PyImport_ImportModule("storm");
        if (!module)
            return 0;

        Undef = PyObject_GetAttrString(module, "Undef");
        if (!Undef)
            return 0;

        Py_DECREF(module);
    }

    {
        module = PyImport_ImportModule("storm.variables");
        if (!module)
            return 0;

        raise_none_error = PyObject_GetAttrString(module, "raise_none_error");
        if (!raise_none_error)
            return 0;

        LazyValue = PyObject_GetAttrString(module, "LazyValue");
        if (!LazyValue)
            return 0;

        Py_DECREF(module);
    }

    {
        module = PyImport_ImportModule("storm.info");
        if (!module)
            return 0;
        
        get_cls_info = PyObject_GetAttrString(module, "get_cls_info");
        if (!get_cls_info)
            return 0;

        Py_DECREF(module);
    }

    {
        module = PyImport_ImportModule("storm.event");
        if (!module)
            return 0;

        EventSystem = PyObject_GetAttrString(module, "EventSystem");
        if (!EventSystem)
            return 0;

        Py_DECREF(module);
    }

    return 1;
}


static PyObject *
Variable_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    VariableObject *self = (VariableObject *)type->tp_alloc(type, 0);

    if (!initialize_globals())
        return NULL;

    /* The following are defined as class properties, so we must initialize
       them here for methods to work with the same logic. */
    Py_INCREF(Undef);
    self->_value = Undef;
    Py_INCREF(Undef);
    self->_lazy_value = Undef;
    Py_INCREF(Undef);
    self->_checkpoint_state = Undef;
    Py_INCREF(Py_True);
    self->_allow_none = Py_True;
    Py_INCREF(Py_None);
    self->event = Py_None;
    Py_INCREF(Py_None);
    self->column = Py_None;

    return (PyObject *)self;
}

static int
Variable_init(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"value", "value_factory", "from_db", "allow_none",
                             "column", "event", NULL};

    PyObject *value = Undef;
    PyObject *value_factory = Undef;
    PyObject *from_db = Py_False;
    PyObject *allow_none = Py_True;
    PyObject *column = Py_None;
    PyObject *event = Py_None;
    PyObject *result;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOOOOO", kwlist, &value,
                                     &value_factory, &from_db, &allow_none,
                                     &column, &event))
        return -1;

    /* if not allow_none: */
    if (allow_none != Py_True &&
        (allow_none == Py_False || !PyObject_IsTrue(allow_none))) {
          /* self._allow_none = False */
          Py_INCREF(Py_False);
          self->_allow_none = Py_False;
    }

    /* if value is not Undef: */
    if (value != Undef) {
        /* self.set(value, from_db) */
        result = PyObject_CallMethod((PyObject *)self,
                                     "set", "OO", value, from_db);
        if (!result) return -1;
    }
    /* elif value_factory is not Undef: */
    else if (value_factory != Undef) {
        /* self.set(value_factory(), from_db) */
        value = PyObject_CallFunctionObjArgs(value_factory, NULL);
        result = PyObject_CallMethod((PyObject *)self,
                                     "set", "OO", value, from_db);
        if (!result) return -1;
    }
    
    /* self.column = column */
    Py_INCREF(column);
    self->column = column;

    /* self.event = event */
    Py_INCREF(event);
    self->event = event;

    return 0;
}

static int
Variable_traverse(VariableObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->_value);
    Py_VISIT(self->_lazy_value);
    Py_VISIT(self->_checkpoint_state);
    /* Py_VISIT(self->_allow_none); */
    Py_VISIT(self->column);
    Py_VISIT(self->event);
    return 0;
}

static int
Variable_clear(VariableObject *self)
{
    Py_CLEAR(self->_value);
    Py_CLEAR(self->_lazy_value);
    Py_CLEAR(self->_checkpoint_state);
    Py_CLEAR(self->_allow_none);
    Py_CLEAR(self->column);
    Py_CLEAR(self->event);
    return 0;
}

static void
Variable_dealloc(VariableObject *self)
{
    Variable_clear(self);
    self->ob_type->tp_free((PyObject *)self);
}

static long
Variable_hash(VariableObject *self)
{
    /* return hash(self._value) */
    return PyObject_Hash((PyObject *)self->_value);
}

static PyObject *
Variable_richcompare(VariableObject *self, VariableObject *other, int op)
{
    /*
       return (self.__class__ is other.__class__ and
               self._value == other._value)
    */
    /* This test will also prevent that we access _value on a
       non-Variable object. */
    if (op == Py_EQ &&
        ((PyObject *)self)->ob_type != ((PyObject *)other)->ob_type) {
        Py_INCREF(Py_False);
        return Py_False;
    }
    return PyObject_RichCompare(self->_value, other->_value, op);
}

static PyObject *
Variable__parse_get(VariableObject *self, PyObject *args)
{
    /* return value */
    PyObject *value, *to_db;
    if (!PyArg_ParseTuple(args, "OO:_parse_get", &value, &to_db))
        return NULL;
    Py_INCREF(value);
    return value;
}

static PyObject *
Variable__parse_set(VariableObject *self, PyObject *args)
{
    /* return value */
    PyObject *value, *from_db;
    if (!PyArg_ParseTuple(args, "OO:_parse_set", &value, &from_db))
        return NULL;
    Py_INCREF(value);
    return value;
}

static PyObject *
Variable_get_lazy(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"default", NULL};
    PyObject *default_ = Py_None;
    PyObject *result;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O:get_lazy", kwlist,
                                     &default_))
        return NULL;

    /* 
       if self._lazy_value is Undef:
           return default
       return self._lazy_value
    */
    if (self->_lazy_value == Undef) {
        result = default_;
    } else {
        result = self->_lazy_value;
    }
    Py_INCREF(result);
    return result;
}

static PyObject *
Variable_get(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"default", "to_db", NULL};
    PyObject *default_ = Py_None;
    PyObject *to_db = Py_False;
    PyObject *result;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OO:get", kwlist,
                                     &default_, &to_db))
        return NULL;

    /* if self._lazy_value is not Undef and self.event is not None: */
    if (self->_lazy_value != Undef && self->event != Py_None) {
        /* self.event.emit("resolve-lazy-value", self, self._lazy_value) */
        result = PyObject_CallMethod(self->event, "emit", "sOO",
                                     "resolve-lazy-value", self,
                                     self->_lazy_value);
        if (!result)
            return NULL;
        Py_DECREF(result);
    }

    /* value = self->_value */
    /* if value is Undef: */
    if (self->_value == Undef) {
        /* return default */
        Py_INCREF(default_);
        return default_;
    }

    /* if value is None: */
    if (self->_value == Py_None) {
        /* return None */
        Py_INCREF(Py_None);
        return Py_None;
    }

    /* return self._parse_get(value, to_db) */
    return PyObject_CallMethod((PyObject *)self, "_parse_get",
                               "OO", self->_value, to_db);
}

static PyObject *
Variable_set(VariableObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"value", "from_db", NULL};
    PyObject *value = Py_None;
    PyObject *from_db = Py_False;
    PyObject *old_value = NULL;
    PyObject *new_value = NULL;
    PyObject *result;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OO:set", kwlist,
                                     &value, &from_db))
        return NULL;

    Py_INCREF(value);

    /* if isinstance(value, LazyValue): */
    if (PyObject_IsInstance(value, LazyValue)) {
        /* self._lazy_value = value */
        Py_INCREF(value);
        Py_DECREF(self->_lazy_value);
        self->_lazy_value = value;

        /* new_value = Undef */
        Py_INCREF(Undef);
        new_value = Undef;
    }
    /* else: */
    else {
        /* self._lazy_value = Undef */
        Py_INCREF(Undef);
        Py_DECREF(self->_lazy_value);
        self->_lazy_value = Undef;

        /* if value is None: */
        if (value == Py_None) {
            /* if self._allow_none is False: */
            if (self->_allow_none == Py_False) {
                /* raise_none_error(self.column) */
                result = PyObject_CallFunctionObjArgs(raise_none_error,
                                                      self->column, NULL);
                /* result should always be NULL here. */
                Py_XDECREF(result);
                goto error;
            }

            /* new_value = None */
            Py_INCREF(Py_None);
            new_value = Py_None;
        }
        /* else: */
        else {
            /* new_value = self._parse_set(value, from_db) */
            new_value = PyObject_CallMethod((PyObject *)self, "_parse_set",
                                            "OO", value, from_db);
            if (!new_value)
                goto error;

            /* if from_db: */
            if (PyObject_IsTrue(from_db)) {
                /* value = self._parse_get(new_value, False) */
                Py_DECREF(value);
                value = PyObject_CallMethod((PyObject *)self, "_parse_get",
                                            "OO", new_value, Py_False);
                if (!value)
                    goto error;
            }
        }
    }

    /* old_value = self._value */
    old_value = self->_value;
    /* Keep the reference with old_value. */

    /* self._value = new_value */
    Py_INCREF(new_value);
    self->_value = new_value;

    /* if (self.event is not None and
           (self._lazy_value is not Undef or new_value != old_value)): */
    if (self->event != Py_None &&
        (self->_lazy_value != Undef ||
         PyObject_RichCompareBool(new_value, old_value, Py_NE))) {

        /* if old_value is not None and old_value is not Undef: */
        if (old_value != Py_None && old_value != Undef) {
            /* old_value = self._parse_get(old_value, False) */
            result = PyObject_CallMethod((PyObject *)self, "_parse_get",
                                         "OO", old_value, Py_False);
            if (!result)
                goto error;

            Py_DECREF(old_value);
            old_value = result;
        }
        /* self.event.emit("changed", self, old_value, value, from_db) */
        result = PyObject_CallMethod((PyObject *)self->event, "emit", "sOOOO",
                                     "changed", self, old_value, value,
                                     from_db);
        if (!result)
            goto error;
        Py_DECREF(result);
    }

    Py_DECREF(value);
    Py_DECREF(old_value);
    Py_DECREF(new_value);
    Py_INCREF(Py_None);
    return Py_None;

error:
    Py_XDECREF(value);
    Py_XDECREF(old_value);
    Py_XDECREF(new_value);
    return NULL;
}

static PyObject *
Variable_delete(VariableObject *self, PyObject *args)
{
    /* old_value = self._value */
    PyObject *old_value = self->_value;
    PyObject *result;

    /* if old_value is not Undef: */
    if (old_value != Undef) {

        /* self._value = Undef */
        Py_INCREF(Undef);
        self->_value = Undef;

        /* if self.event is not None: */
        if (self->event != Py_None) {
            /* if old_value is not None and old_value is not Undef: */
            if (old_value != Py_None && old_value != Undef) {
                /* old_value = self._parse_get(old_value, False) */
                result = PyObject_CallMethod((PyObject *)self, "_parse_get",
                                             "OO", old_value, Py_False);
                if (!result)
                    return NULL;

                Py_DECREF(old_value);
                old_value = result;
            }

            /* self.event.emit("changed", self, old_value, Undef, False) */
            result = PyObject_CallMethod((PyObject *)self->event, "emit",
                                         "sOOOO", "changed", self, old_value,
                                         Undef, Py_False);
            if (!result)
                return NULL;
            Py_DECREF(result);
        }
    }

    Py_DECREF(old_value);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
Variable_is_defined(VariableObject *self, PyObject *args)
{
    /* return self._value is not Undef */
    return PyBool_FromLong(self->_value != Undef);
}

static PyObject *
Variable_has_changed(VariableObject *self, PyObject *args)
{
    /* return (self._lazy_value is not Undef or
               self.get_state() != self._checkpoint_state) */
    PyObject *result = Py_True;
    if (self->_lazy_value == Undef) {
        PyObject *state = PyObject_CallMethod((PyObject *)self,
                                              "get_state", NULL);
        int res;
        if (state == NULL)
            return NULL;
        res = PyObject_RichCompareBool(state, self->_checkpoint_state, Py_EQ);
        Py_DECREF(state);
        if (res == -1)
            return NULL;
        if (res)
            result = Py_False;
    }
    Py_INCREF(result);
    return result;
}

static PyObject *
Variable_get_state(VariableObject *self, PyObject *args)
{
    /* return self._value is not Undef */
    PyObject *result = PyTuple_New(2);
    if (!result)
        return NULL;
    Py_INCREF(self->_lazy_value);
    Py_INCREF(self->_value);
    PyTuple_SET_ITEM(result, 0, self->_lazy_value);
    PyTuple_SET_ITEM(result, 1, self->_value);
    return result;
}

static PyObject *
Variable_set_state(VariableObject *self, PyObject *args)
{
    /* self._lazy_value, self._value = state */
    if (!PyArg_ParseTuple(args, "(OO):set_state",
                          &self->_lazy_value, &self->_value))
        return NULL;
    Py_INCREF(self->_lazy_value);
    Py_INCREF(self->_value);
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
Variable_checkpoint(VariableObject *self, PyObject *args)
{
    /* self._checkpoint_state = self.get_state() */
    PyObject *state = PyObject_CallMethod((PyObject *)self, "get_state", NULL);
    if (!state)
        return NULL;
    Py_DECREF(self->_checkpoint_state);
    self->_checkpoint_state = state;
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
Variable_copy(VariableObject *self, PyObject *args)
{
    PyObject *noargs, *variable, *state, *result;
    /* variable = self.__class__.__new__(self.__class__) */
    noargs = PyTuple_New(0);
    variable = self->ob_type->tp_new(self->ob_type, noargs, NULL);
    Py_DECREF(noargs);
    if (!variable)
        return NULL;
    /* variable.set_state(self.get_state()) */
    state = PyObject_CallMethod((PyObject *)self, "get_state", NULL);
    if (!state) {
        Py_DECREF(variable);
        return NULL;
    }
    result = PyObject_CallMethod((PyObject *)variable,
                                 "set_state", "(O)", state);
    Py_DECREF(state);
    if (!result) {
        Py_DECREF(variable);
        return NULL;
    }
    Py_DECREF(result);
    return variable;
}

static PyMethodDef Variable_methods[] = {
    {"_parse_get", (PyCFunction)Variable__parse_get, METH_VARARGS, NULL},
    {"_parse_set", (PyCFunction)Variable__parse_set, METH_VARARGS, NULL},
    {"get_lazy", (PyCFunction)Variable_get_lazy,
        METH_VARARGS | METH_KEYWORDS, NULL},
    {"get", (PyCFunction)Variable_get, METH_VARARGS | METH_KEYWORDS, NULL},
    {"set", (PyCFunction)Variable_set, METH_VARARGS | METH_KEYWORDS, NULL},
    {"delete", (PyCFunction)Variable_delete,
        METH_VARARGS | METH_KEYWORDS, NULL},
    {"is_defined", (PyCFunction)Variable_is_defined, METH_NOARGS, NULL},
    {"has_changed", (PyCFunction)Variable_has_changed, METH_NOARGS, NULL},
    {"get_state", (PyCFunction)Variable_get_state, METH_NOARGS, NULL},
    {"set_state", (PyCFunction)Variable_set_state, METH_VARARGS, NULL},
    {"checkpoint", (PyCFunction)Variable_checkpoint, METH_NOARGS, NULL},
    {"copy", (PyCFunction)Variable_copy, METH_NOARGS, NULL},
    {NULL, NULL}
};

#define OFFSETOF(x) offsetof(VariableObject, x)
static PyMemberDef Variable_members[] = {
    {"_value", T_OBJECT, OFFSETOF(_value), 0, 0},
    {"_lazy_value", T_OBJECT, OFFSETOF(_lazy_value), 0, 0},
    {"_checkpoint_state", T_OBJECT, OFFSETOF(_checkpoint_state), 0, 0},
    {"_allow_none", T_OBJECT, OFFSETOF(_allow_none), 0, 0},
    {"column", T_OBJECT, OFFSETOF(column), 0, 0},
    {"event", T_OBJECT, OFFSETOF(event), 0, 0},
    {NULL}
};
#undef OFFSETOF

statichere PyTypeObject Variable_Type = {
	PyObject_HEAD_INIT(NULL)
	0,			/*ob_size*/
	"storm.variables.Variable",	/*tp_name*/
	sizeof(VariableObject), /*tp_basicsize*/
	0,			/*tp_itemsize*/
	(destructor)Variable_dealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	0,			/*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,          /*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	0,			/*tp_as_mapping*/
	(hashfunc)Variable_hash, /*tp_hash*/
    0,                      /*tp_call*/
    0,                      /*tp_str*/
    PyObject_GenericGetAttr,/*tp_getattro*/
    PyObject_GenericSetAttr,/*tp_setattro*/
    0,                      /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    0,                      /*tp_doc*/
    (traverseproc)Variable_traverse,  /*tp_traverse*/
    (inquiry)Variable_clear,          /*tp_clear*/
    (richcmpfunc)Variable_richcompare, /*tp_richcompare*/
    0,                      /*tp_weaklistoffset*/
    0,                      /*tp_iter*/
    0,                      /*tp_iternext*/
    Variable_methods,        /*tp_methods*/
    Variable_members,        /*tp_members*/
    0,                      /*tp_getset*/
    0,                      /*tp_base*/
    0,                      /*tp_dict*/
    0,                      /*tp_descr_get*/
    0,                      /*tp_descr_set*/
    0,                      /*tp_dictoffset*/
    (initproc)Variable_init, /*tp_init*/
    PyType_GenericAlloc,    /*tp_alloc*/
    Variable_new,      /*tp_new*/
    PyObject_GC_Del,        /*tp_free*/
    0,                      /*tp_is_gc*/
};


static PyObject *
ObjectInfo__emit_object_deleted(ObjectInfoObject *self, PyObject *args)
{
    /* self.event.emit("object-deleted") */
    return PyObject_CallMethod(self->event, "emit", "s", "object-deleted");
}

static PyMethodDef ObjectInfo_deleted_callback =
    {"_emit_object_deleted", (PyCFunction)ObjectInfo__emit_object_deleted,
        METH_O, NULL};

static int
ObjectInfo_init(ObjectInfoObject *self, PyObject *args)
{
    PyObject *empty_args = NULL;
    PyObject *factory_kwargs = NULL;
    PyObject *columns = NULL;
    PyObject *primary_key = NULL;
    PyObject *obj;
    Py_ssize_t i;

    empty_args = PyTuple_New(0);
    if (!empty_args)
        goto error;

    if (PyDict_Type.tp_init((PyObject *)self, empty_args, NULL) == -1)
        goto error;

    if (!initialize_globals())
        goto error;

    if (!PyArg_ParseTuple(args, "O", &obj))
        goto error;

    /* self.cls_info = get_cls_info(type(obj)) */
    self->cls_info = PyObject_CallFunctionObjArgs(get_cls_info, obj->ob_type,
                                                  NULL);
    if (!self->cls_info)
        goto error;

    /* self.set_obj(obj) */
    /* This is slightly different. get_obj() is an actual method. */
    self->__obj_ref_callback = PyCFunction_NewEx(&ObjectInfo_deleted_callback,
                                                 (PyObject *)self, NULL);
    if (!self->__obj_ref_callback)
        goto error;

    self->__obj_ref = PyWeakref_NewRef(obj, self->__obj_ref_callback);
    if (!self->__obj_ref)
        goto error;

    /* self.event = event = EventSystem(self) */
    self->event = PyObject_CallFunctionObjArgs(EventSystem, self, NULL);
    if (!self->event)
        goto error;

    /* self->variables = variables = {} */
    self->variables = PyDict_New();
    if (!self->variables)
        goto error;

    factory_kwargs = PyDict_New();
    if (!factory_kwargs)
        goto error;

    if (PyDict_SetItemString(factory_kwargs, "event", self->event) == -1)
        goto error;

    /* for column in self.cls_info.columns: */
    columns = PyObject_GetAttrString(self->cls_info, "columns");
    if (!columns)
        goto error;
    for (i = 0; i != PyTuple_GET_SIZE(columns); i++) {
        /* variables[column] = column.variable_factory(column=column,
                                                       event=event) */
        PyObject *column = PyTuple_GET_ITEM(columns, i);
        PyObject *variable, *factory;
        if (PyDict_SetItemString(factory_kwargs, "column", column) == -1)
            goto error;
        factory = PyObject_GetAttrString(column, "variable_factory");
        if (!factory)
            goto error;
        variable = PyObject_Call(factory, empty_args, factory_kwargs);
        Py_DECREF(factory);
        if (!variable)
            goto error;
        if (PyDict_SetItem(self->variables, column, variable) == -1) {
            Py_DECREF(variable);
            goto error;
        }
        Py_DECREF(variable);
    }

    /* self.primary_vars = tuple(variables[column]
                                 for column in self.cls_info.primary_key) */
    primary_key = PyObject_GetAttrString((PyObject *)self->cls_info,
                                         "primary_key");
    if (!primary_key)
        goto error;
    /* XXX Check primary_key type here. */
    self->primary_vars = PyTuple_New(PyTuple_GET_SIZE(primary_key));
    if (!self->primary_vars)
        goto error;
    for (i = 0; i != PyTuple_GET_SIZE(primary_key); i++) {
        PyObject *column = PyTuple_GET_ITEM(primary_key, i);
        PyObject *variable = PyDict_GetItem(self->variables, column);
        Py_INCREF(variable);
        PyTuple_SET_ITEM(self->primary_vars, i, variable);
    }

    Py_DECREF(empty_args);
    Py_DECREF(factory_kwargs);
    Py_DECREF(columns);
    Py_DECREF(primary_key);
    return 0;

error:
    Py_XDECREF(empty_args);
    Py_XDECREF(factory_kwargs);
    Py_XDECREF(columns);
    Py_XDECREF(primary_key);
    return -1;
}

static PyObject *
ObjectInfo_get_obj(ObjectInfoObject *self, PyObject *args)
{
    PyObject *obj = PyWeakref_GET_OBJECT(self->__obj_ref);
    Py_INCREF(obj);
    return obj;
}

static PyObject *
ObjectInfo_set_obj(ObjectInfoObject *self, PyObject *args)
{
    PyObject *obj;

    /* self.get_obj = ref(obj, self._emit_object_deleted) */
    /* This is slightly different. get_obj() is an actual method. */
    if (!PyArg_ParseTuple(args, "O", &obj))
        return NULL;

    Py_DECREF(self->__obj_ref);
    self->__obj_ref = PyWeakref_NewRef(obj, self->__obj_ref_callback);
    if (!self->__obj_ref)
        return NULL;

    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
ObjectInfo_checkpoint(ObjectInfoObject *self, PyObject *args)
{
    PyObject *column, *variable, *result;
    Py_ssize_t i = 0;

    /* for variable in self.variables.itervalues(): */
    while (PyDict_Next(self->variables, &i, &column, &variable)) {
        /* variable.checkpoint() */
        result = PyObject_CallMethod(variable, "checkpoint", NULL);
        if (!result) return NULL;
    }
    Py_INCREF(Py_None);
    return Py_None;
}

static PyObject *
ObjectInfo__storm_object_info__(PyObject *self, void *closure)
{
    /* __storm_object_info__ = property(lambda self:self) */
    Py_INCREF(self);
    return self;
}

static int
ObjectInfo_traverse(ObjectInfoObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->__obj_ref);
    Py_VISIT(self->__obj_ref_callback);
    Py_VISIT(self->cls_info);
    Py_VISIT(self->event);
    Py_VISIT(self->variables);
    Py_VISIT(self->primary_vars);
    return PyDict_Type.tp_traverse((PyObject *)self, visit, arg);
}

static int
ObjectInfo_clear(ObjectInfoObject *self)
{
    Py_CLEAR(self->__obj_ref);
    Py_CLEAR(self->__obj_ref_callback);
    Py_CLEAR(self->cls_info);
    Py_CLEAR(self->event);
    Py_CLEAR(self->variables);
    Py_CLEAR(self->primary_vars);
    return PyDict_Type.tp_clear((PyObject *)self);
}

static void
ObjectInfo_dealloc(ObjectInfoObject *self)
{
	if (self->__weakreflist)
		PyObject_ClearWeakRefs((PyObject *)self);
    Py_CLEAR(self->__obj_ref);
    Py_CLEAR(self->__obj_ref_callback);
    Py_CLEAR(self->cls_info);
    Py_CLEAR(self->event);
    Py_CLEAR(self->variables);
    Py_CLEAR(self->primary_vars);
    return PyDict_Type.tp_dealloc((PyObject *)self);
}

static PyMethodDef ObjectInfo_methods[] = {
    {"_emit_object_deleted", (PyCFunction)ObjectInfo__emit_object_deleted,
        METH_O, NULL},
    {"get_obj", (PyCFunction)ObjectInfo_get_obj, METH_NOARGS, NULL},
    {"set_obj", (PyCFunction)ObjectInfo_set_obj, METH_VARARGS, NULL},
    {"checkpoint", (PyCFunction)ObjectInfo_checkpoint, METH_VARARGS, NULL},
    {NULL, NULL}
};

#define OFFSETOF(x) offsetof(ObjectInfoObject, x)
static PyMemberDef ObjectInfo_members[] = {
    {"cls_info", T_OBJECT, OFFSETOF(cls_info), 0, 0},
    {"event", T_OBJECT, OFFSETOF(event), 0, 0},
    {"variables", T_OBJECT, OFFSETOF(variables), 0, 0},
    {"primary_vars", T_OBJECT, OFFSETOF(primary_vars), 0, 0},
    {NULL}
};
#undef OFFSETOF

static PyGetSetDef ObjectInfo_getset[] = {
    {"__storm_object_info__", (getter)ObjectInfo__storm_object_info__,
        NULL, NULL},
    {NULL}
};

statichere PyTypeObject ObjectInfo_Type = {
	PyObject_HEAD_INIT(NULL)
	0,			/*ob_size*/
	"storm.info.ObjectInfo", /*tp_name*/
	sizeof(ObjectInfoObject), /*tp_basicsize*/
	0,			/*tp_itemsize*/
	(destructor)ObjectInfo_dealloc, /*tp_dealloc*/
	0,			/*tp_print*/
	0,			/*tp_getattr*/
	0,			/*tp_setattr*/
	0,			/*tp_compare*/
	0,          /*tp_repr*/
	0,			/*tp_as_number*/
	0,			/*tp_as_sequence*/
	0,			/*tp_as_mapping*/
	(hashfunc)_Py_HashPointer, /*tp_hash*/
    0,                      /*tp_call*/
    0,                      /*tp_str*/
    PyObject_GenericGetAttr, /*tp_getattro*/
    PyObject_GenericSetAttr, /*tp_setattro*/
    0,                      /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE|Py_TPFLAGS_HAVE_GC, /*tp_flags*/
    0,                      /*tp_doc*/
    (traverseproc)ObjectInfo_traverse, /*tp_traverse*/
    (inquiry)ObjectInfo_clear, /*tp_clear*/
    0,                      /*tp_richcompare*/
    offsetof(ObjectInfoObject, __weakreflist), /*tp_weaklistoffset*/
    0,                      /*tp_iter*/
    0,                      /*tp_iternext*/
    ObjectInfo_methods,     /*tp_methods*/
    ObjectInfo_members,     /*tp_members*/
    ObjectInfo_getset,      /*tp_getset*/
    0,                      /*tp_base*/
    0,                      /*tp_dict*/
    0,                      /*tp_descr_get*/
    0,                      /*tp_descr_set*/
    0,                      /*tp_dictoffset*/
    (initproc)ObjectInfo_init, /*tp_init*/
    PyType_GenericAlloc,    /*tp_alloc*/
    0,                      /*tp_new*/
    PyObject_GC_Del,        /*tp_free*/
    0,                      /*tp_is_gc*/
};


static PyObject *
get_obj_info(PyObject *self, PyObject *obj)
{
    PyObject *obj_info;

    if (obj->ob_type == &ObjectInfo_Type) {
        /* Much better than asking the ObjectInfo to return itself. ;-) */
        Py_INCREF(obj);
        return obj;
    }

    /* try:
          return obj.__storm_object_info__ */
    obj_info = PyObject_GetAttrString(obj, "__storm_object_info__");

    /* except AttributeError: */
    if (obj_info == NULL) {
        PyErr_Clear();

        /* obj_info = ObjectInfo(obj) */
        obj_info = PyObject_CallFunctionObjArgs((PyObject *)&ObjectInfo_Type,
                                                obj, NULL);
        if (!obj_info)
            return NULL;

        /* return obj.__dict__.setdefault("__storm_object_info__", obj_info) */
        if (PyObject_SetAttrString(obj, "__storm_object_info__",
                                   obj_info) == -1)
            return NULL;
    }

    return obj_info;
}


static PyMethodDef cextensions_methods[] = {
    {"get_obj_info", (PyCFunction)get_obj_info, METH_O, NULL},
    {NULL, NULL}
};


DL_EXPORT(void)
initcextensions(void)
{
    PyObject *module;

    PyType_Ready(&Variable_Type);

    ObjectInfo_Type.tp_base = &PyDict_Type;
    PyType_Ready(&ObjectInfo_Type);

    module = Py_InitModule3("cextensions", cextensions_methods, "");
    Py_INCREF(&Variable_Type);

#define REGISTER_TYPE(name) \
    do { \
        Py_INCREF(&name##_Type); \
        PyModule_AddObject(module, #name, (PyObject*)&name##_Type); \
    } while(0)

    REGISTER_TYPE(Variable);
    REGISTER_TYPE(ObjectInfo);
}

/* vim:ts=4:sw=4:et
*/
