class Column(object):
    def __init__(self, name):
        if not isinstance(name, str):
            self.name = f"({name})"
        elif name.startswith("("):
            self.name = name
        else:
            self.name = "{" + name + "}"

    def alias(self, alias):
        self._alias = alias

    def __str__(self):
        return "Column \"" + self.name + "\""

    def __neg__(self):
        return _un_op("negate", self)

    def __add__(self, other):
        return _bin_op("add", self, other)
    
    def __sub__(self, other):
        return _bin_op("subtract", self, other)

    def __mul__(self, other):
        return _bin_op("multiply", self, other)

    def __truediv__(self, other):
        return _bin_op("divide", self, other)

    def __lt__(self, other):
        return _bin_op("less", self, other)

    def __le__(self, other):
        return _bin_op("less_equal", self, other)

    def __eq__(self, other):
        return _bin_op("equal", self, other)

    def __ne__(self, other):
        return _bin_op("not_equal", self, other)

    def __gt__(self, other):
        return _bin_op("greater", self, other)

    def __ge__(self, other):
        return _bin_op("greater_equal", self, other)

def lit(val):
    if isinstance(val, str):
        val = "\"" + val + "\""
    return Column("(pc.scalar({}))".format(val))

def col(name):
    return Column(name)

def _un_op(op, a):
        if not isinstance(a, Column):
            a = Column(a)
        return Column("(pc.{}({}))".format(op, a.name))

def _sc_op(op, a, sc):
        if not isinstance(a, Column):
            a = Column(a)
        return Column("(pc.{}({}, {}))".format(op, a.name, sc))

def _bin_op(op, a, b):
    if not isinstance(a, Column):
        a = Column(a)
    if not isinstance(b, Column):
        b = Column(b)
    return Column("(pc.{}({}, {}))".format(op, a.name, b.name))

def _multi_op(op, *cols):
    cols_a = []
    for c in cols:
        if not isinstance(c, Column):
            c = Column(c)
        cols_a.append(c)
    return Column("(pc.{}({}))".format(op, ", ".join([c.name for c in cols_a])))

def abs(a):
    return _un_op("abs", a)

def sign(a):
    return _un_op("sign", a)

def round(a, ndigits):
    return _sc_op("round", a, ndigits)

def greatest(*cols):
    return _multi_op("max_element_wise", *cols)

def least(*cols):
    return _multi_op("min_element_wise", *cols)

def coalesce(*cols):
    return _multi_op("coalesce", *cols)
    

def struct(*cols):
    cols_a = []
    for c in cols:
        if not isinstance(c, Column):
            c = Column(c)
        cols_a.append(c)
    cl = Column("(pc.make_struct({0}, field_names=[{1}]))".format(", ".join([c.name for c in cols_a]), ", ".join([c.name.replace("{", "\"").replace("}", "\"") for c in cols_a])))
    #print(cl.name)
    return cl