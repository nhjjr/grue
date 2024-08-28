import classad
import htcondor

from grue.utils import (
    camel_to_snake, snake_to_camel, translate_classad, ClassAdCollector)


def test_camel_to_snake():
    test_values = {
        'FooBarBaz': 'foo_bar_baz', 'fooBarBaz': 'foo_bar_baz',
        'FOOBarBaz': 'foobar_baz', 'foo_bar_baz': 'foo_bar_baz',
        'foo_bar_BAZ': 'foo_bar_baz', '_foo_bar_baz': '_foo_bar_baz',
        'fooBARbaz': 'foo_barbaz', 'FOOBARBAZ': 'foobarbaz',
        'Foo1Bar2Baz3': 'foo1bar2baz3', '123': '123', '1_2_3': '1_2_3'}

    for arg, expected in test_values.items():
        assert camel_to_snake(arg) == expected


def test_snake_to_camel():
    test_values = {
        'foo_bar_baz': 'FooBarBaz', '_foo_bar_baz_': 'FooBarBaz',
        'foo__bar_baz': 'FooBarBaz', 'FooBarBaz': 'Foobarbaz',
        'FOOBARBAZ': 'Foobarbaz', 'foo1_bar2_baz3': 'Foo1Bar2Baz3',
        '123': '123', '1_2_3': '123'}

    for arg, expected in test_values.items():
        assert snake_to_camel(arg) == expected


def test_translate_classad():
    ad = {'foo': 1, 'bar_baz': 'foo'}
    assert isinstance(translate_classad(ad, func=snake_to_camel), dict)

    # Regular classad
    ad = {'foo': 1, 'bar_baz': 'foo'}
    ad_t = {'Foo': 1, 'BarBaz': 'foo'}
    assert translate_classad(ad, func=snake_to_camel) == ad_t

    # Multilevel classad
    ad = {'foo': 1, 'bar_baz': {'grue_grue': 2, 'baz_grue': 3}}
    ad_t = {'Foo': 1, 'BarBaz': {'GrueGrue': 2, 'BazGrue': 3}}
    assert translate_classad(ad, func=snake_to_camel) == ad_t

    # Int-as-Key classad
    ad = {'foo': 1, 1: 2, '2': 3, 4: 5}
    ad_t = {'Foo': 1, 1: 2, '2': 3, 4: 5}
    assert translate_classad(ad, func=snake_to_camel) == ad_t

    # dict hidden in list classad
    ad = {'one_two_THREE': [{'foo_bar': 1, 'bar_foo': {1: 'a_a', 'a_a': 1}}]}
    ad_t = {'OneTwoThree': [{'FooBar': 1, 'BarFoo': {1: 'a_a', 'AA': 1}}]}
    assert translate_classad(ad, func=snake_to_camel) == ad_t


def test_classad_collector(monkeypatch):
    def mock_query(*args, **kwargs):
        return [classad.classad.ClassAd({'foo': 'bar'})]

    collector = ClassAdCollector(ad_type=htcondor.AdTypes.Startd)

    # Constraint test
    collector.constraint = 'some constraint'
    collector.constraint_or('other constraint')
    assert collector.constraint == 'some constraint || other constraint'

    collector.constraint = 'some constraint'
    collector.constraint_and('other constraint')
    assert collector.constraint == 'some constraint && other constraint'

    # Projection test
    collector.projection = ['some', 'projection']
    assert collector.projection == ['some', 'projection']

    monkeypatch.setattr(htcondor.Collector, 'query', mock_query)
    collector.constraint = 'some constraint'
    collector.projection = ['some', 'projection']
    collector.fetch()
    assert collector._classads == [classad.classad.ClassAd({'foo': 'bar'})]

    # json (dict) conversion test
    expected_json = {repr(collector._ad_type): [{'foo': 'bar'}]}
    assert collector.json_classads == expected_json
