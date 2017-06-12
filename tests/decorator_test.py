# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import print_function

import datetime
import unittest

import luigi
import luigi.notifications
from luigi.parameter import MissingParameterException
from luigi.util import common_params

from bioluigi.decorators import requires, inherits


luigi.notifications.DEBUG = True


class A(luigi.Task):
    param1 = luigi.Parameter("class A-specific default")


@inherits(A)
class B(luigi.Task):
    param2 = luigi.Parameter("class B-specific default")


@inherits(B)
class C(luigi.Task):
    param3 = luigi.Parameter("class C-specific default")


@inherits(B)
class D(luigi.Task):
    param1 = luigi.Parameter("class D overwriting class A's default")


@inherits(B)
class D_null(luigi.Task):
    param1 = None


@inherits(A)
@inherits(B)
class E(luigi.Task):
    param4 = luigi.Parameter("class E-specific default")


@inherits(A, B)
class E_list(luigi.Task):
    param4 = luigi.Parameter("class E-specific default")


@inherits(A=A, B=B)
class E_dict(luigi.Task):
    param4 = luigi.Parameter("class E-specific default")


class InheritTest(unittest.TestCase):

    def setUp(self):
        self.a = A()
        self.a_changed = A(param1=34)
        self.b = B()
        self.c = C()
        self.d = D()
        self.d_null = D_null()
        self.e = E()
        self.e_list = E_list()
        self.e_dict = E_dict()

    def test_has_param(self):
        b_params = dict(self.b.get_params()).keys()
        self.assertTrue("param1" in b_params)

    def test_default_param(self):
        self.assertEqual(self.b.param1, self.a.param1)

    def test_change_of_defaults_not_equal(self):
        self.assertNotEqual(self.b.param1, self.a_changed.param1)

    def tested_chained_inheritance(self):
        self.assertEqual(self.c.param2, self.b.param2)
        self.assertEqual(self.c.param1, self.a.param1)
        self.assertEqual(self.c.param1, self.b.param1)

    def test_overwriting_defaults(self):
        self.assertEqual(self.d.param2, self.b.param2)
        self.assertNotEqual(self.d.param1, self.b.param1)
        self.assertNotEqual(self.d.param1, self.a.param1)
        self.assertEqual(self.d.param1, "class D overwriting class A's default")

    def test_stacked_inheritance(self):
        self.assertEqual(self.e.param1, self.a.param1)
        self.assertEqual(self.e.param1, self.b.param1)
        self.assertEqual(self.e.param2, self.b.param2)

    def test_list_inheritance(self):
        self.assertEqual(self.e_list.param1, self.a.param1)
        self.assertEqual(self.e_list.param1, self.b.param1)
        self.assertEqual(self.e_list.param2, self.b.param2)

    def test_dict_inheritance(self):
        self.assertEqual(self.e_dict.param1, self.a.param1)
        self.assertEqual(self.e_dict.param1, self.b.param1)
        self.assertEqual(self.e_dict.param2, self.b.param2)

    def test_removing_parameter(self):
        self.assertFalse("param1" in dict(self.d_null.get_params()).keys())

    def test_wrapper_preserve_attributes(self):
        self.assertEqual(B.__name__, 'B')


class F(luigi.Task):
    param1 = luigi.Parameter("A parameter on a base task, that will be required later.")


@inherits(F)
class G(luigi.Task):
    param2 = luigi.Parameter("A separate parameter that doesn't affect 'F'")

    def requires(self):
        return F(**common_params(self, F))


@inherits(G)
class H(luigi.Task):
    param2 = luigi.Parameter("OVERWRITING")

    def requires(self):
        return G(**common_params(self, G))


@inherits(G)
class H_null(luigi.Task):
    param2 = None

    def requires(self):
        special_param2 = str(datetime.datetime.now())
        return G(param2=special_param2, **common_params(self, G))


@inherits(G)
class I(luigi.Task):

    def requires(self):
        return F(**common_params(self, F))


class J(luigi.Task):
    param1 = luigi.Parameter()  # something required, with no default


@inherits(J)
class K_shouldnotinstantiate(luigi.Task):
    param2 = luigi.Parameter("A K-specific parameter")


@inherits(J)
class K_shouldfail(luigi.Task):
    param1 = None
    param2 = luigi.Parameter("A K-specific parameter")

    def requires(self):
        return J(**common_params(self, J))


@inherits(J)
class K_shouldsucceed(luigi.Task):
    param1 = None
    param2 = luigi.Parameter("A K-specific parameter")

    def requires(self):
        return J(param1="Required parameter", **common_params(self, J))


@inherits(J)
class K_wrongparamsorder(luigi.Task):
    param1 = None
    param2 = luigi.Parameter("A K-specific parameter")

    def requires(self):
        return J(param1="Required parameter", **common_params(J, self))


@requires(A, B)
class L_list(luigi.Task):
    param4 = luigi.Parameter("class L-specific default")


@requires(A=A, B=B)
class L_dict(luigi.Task):
    param4 = luigi.Parameter("class L-specific default")


class RequiresTest(unittest.TestCase):

    def setUp(self):
        self.f = F()
        self.g = G()
        self.g_changed = G(param1="changing the default")
        self.h = H()
        self.h_null = H_null()
        self.i = I()
        self.k_shouldfail = K_shouldfail()
        self.k_shouldsucceed = K_shouldsucceed()
        self.k_wrongparamsorder = K_wrongparamsorder()
        self.l_list = L_list()
        self.l_dict = L_dict()

    def test_inherits(self):
        self.assertEqual(self.f.param1, self.g.param1)
        self.assertEqual(self.f.param1, self.g.requires().param1)

    def test_change_of_defaults(self):
        self.assertNotEqual(self.f.param1, self.g_changed.param1)
        self.assertNotEqual(self.g.param1, self.g_changed.param1)
        self.assertNotEqual(self.f.param1, self.g_changed.requires().param1)

    def test_overwriting_parameter(self):
        self.h.requires()
        self.assertNotEqual(self.h.param2, self.g.param2)
        self.assertEqual(self.h.param2, self.h.requires().param2)
        self.assertEqual(self.h.param2, "OVERWRITING")

    def test_skipping_one_inheritance(self):
        self.assertEqual(self.i.requires().param1, self.f.param1)

    def test_removing_parameter(self):
        self.assertNotEqual(self.h_null.requires().param2, self.g.param2)

    def test_not_setting_required_parameter(self):
        self.assertRaises(MissingParameterException, self.k_shouldfail.requires)

    def test_setting_required_parameters(self):
        self.k_shouldsucceed.requires()

    def test_should_not_instantiate(self):
        self.assertRaises(MissingParameterException, K_shouldnotinstantiate)

    def test_resuscitation(self):
        k = K_shouldnotinstantiate(param1='hello')
        k.requires()

    def test_wrong_common_params_order(self):
        self.assertRaises(TypeError, self.k_wrongparamsorder.requires)

    def test_list(self):
        self.assertTrue(isinstance(self.l_list.requires(), list))
        self.assertTrue(isinstance(self.l_list.requires()[0], A))
        self.assertTrue(isinstance(self.l_list.requires()[1], B))

    def test_dict(self):
        self.assertTrue(isinstance(self.l_dict.requires(), dict))
        self.assertTrue(isinstance(self.l_dict.requires()['A'], A))
        self.assertTrue(isinstance(self.l_dict.requires()['B'], B))


class X(luigi.Task):
    n = luigi.IntParameter(default=42)


@inherits(X)
class Y(luigi.Task):

    def requires(self):
        return self.clone_parent()


@requires(X)
class Y2(luigi.Task):
    pass


@requires(X)
class Y3(luigi.Task):
    n = luigi.IntParameter(default=43)


class CloneParentTest(unittest.TestCase):

    def test_clone_parent(self):
        y = Y()
        x = X()
        self.assertEqual(y.requires(), x)
        self.assertEqual(y.n, 42)

    def test_requires(self):
        y2 = Y2()
        x = X()
        self.assertEqual(y2.requires(), x)
        self.assertEqual(y2.n, 42)

    def test_requires_override_default(self):
        y3 = Y3()
        x = X()
        self.assertNotEqual(y3.requires(), x)
        self.assertEqual(y3.n, 43)
        self.assertEqual(y3.requires().n, 43)

    def test_names(self):
        # Just make sure the decorators retain the original class names
        x = X()
        self.assertEqual(str(x), 'X(n=42)')
        self.assertEqual(x.__class__.__name__, 'X')
