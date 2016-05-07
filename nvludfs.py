import os
import sys
sys.path.append(os.path.expanduser("./python-rightarrow/"))
sys.path.append(os.path.expanduser("./Meta/"))

from rightarrow.parser import Parser

import rightarrow.annotations as ra

import meta
import ast
    
## NVL

# TODO: instead of returning raw strings, return NvlContext objects

# A cache of already compiled UDFs.
registry = {}

# Already compiled functions
funcs = {}

def nvl_codegen(node, names={}):
  # A dummy codegen; we should replace this with something we already have.
  # TODO Type inference
  if isinstance(node, ast.FunctionDef):
    return nvl_codegen(node.body[0], names)
  if isinstance(node, ast.Return):
    return nvl_codegen(node.value, names)
  if isinstance(node, ast.BinOp):
    if isinstance(node.op, ast.Add):
      return nvl_codegen(node.left, names) +  " + " + nvl_codegen(node.right, names)
    elif isinstance(node.op, ast.Mult):
      return nvl_codegen(node.left, names) +  " * " + nvl_codegen(node.right, names)
    if isinstance(node.op, ast.Sub):
      return nvl_codegen(node.left, names) +  " - " + nvl_codegen(node.right, names)
  if isinstance(node, ast.Name):
    return names[node.id]
  if isinstance(node, ast.Subscript):
    return "lookup({0}, {1})".format(nvl_codegen(node.value, names), nvl_codegen(node.slice, names))
  if isinstance(node, ast.Index):
    return nvl_codegen(node.value, names)
  if isinstance(node, ast.Num):
    return str(node.n) + "L"
  if isinstance(node, ast.List):
    elems = [nvl_codegen(a, names) for a in node.elts]
    return "[" + ", ".join(elems) + "]"
  if isinstance(node, ast.Call):
    # Somewhat hacky, but for now we'll support some of the functional expressions in python
    if node.func.id == 'map':
      nvlFunc = nvl_codegen(node.args[0], names)
      nvlArray = nvl_codegen(node.args[1], names)
      return "map({0}, {1})".format(nvlArray, nvlFunc)
    elif node.func.id == 'zip':
      args = [nvl_codegen(a, names) for a in node.args]
      return "zip({0})".format(",".join(args))
    if node.func.id == 'sum':
      args = [nvl_codegen(a, names) for a in node.args]
      args.append("0")
      args.append("(t0: int, t1: int) => t0 + t1")
      return "agg({0})".format(", ".join(args))

  # Unsupported node
  raise ValueError("Unsupported AST node '{0}'".format(node.__class__.__name__))

def nvl_type(t):
  """
  Convert the rightarrow type to the appropriate NVL type. This should use nvlobjects and nvltypes.
  """
  if isinstance(t, ra.List):
    return "vec[{0}]".format(nvl_type(t.elem_ty))
  elif isinstance(t, ra.NamedType):
    return str(t)
  else:
    raise TypeError("Unsupported type in NVL: {0}".format(t))

def nvl_header(ty, tree, tupargs):
  """
  Return a method header and a set of names derived from the 
  arguments. If tupname is true, the function arguments are wrapped in an NVL struct.
  """
  if not isinstance(tree, ast.FunctionDef):
    raise ValueError("AST root must be FunctionDef")

  assert len(tree.args.args) == len(ty.arg_types)

  var_id = 0

  names = {}

  #print ast.dump(tree)

  # Already compiled NVL compatible functions are valid identifiers
  names.update(funcs)

  tupname = 'ta0'
  for arg in tree.args.args:
    if tupargs:
      names[arg.id] = tupname + "." + str(var_id)
      var_id += 1
    else:
      names[arg.id] = "a" + str(var_id)
      var_id += 1

  i = 0
  hdrToks = []
  for arg_id, arg in zip(range(var_id), ty.arg_types):
    if tupargs:
      hdrToks.append(nvl_type(arg))
    else:
      hdrToks.append("a{0}: {1}".format(arg_id, nvl_type(arg)))

  if tupargs:
    nvlHdr = "(" + tupname + ": {" + ",".join(hdrToks) + "})"
  else:
    nvlHdr = "(" + ", ".join(hdrToks) + ")"

  return nvlHdr, names


def extract_nvl(ty, func, tupargs):
  tree = meta.decompiler.decompile_func(func)
  #print ast.dump(tree)
  # Codegen NVL here using tree. tree is a Python AST; we should
  # basically write a "codegen" for the nodes we want to support.
  # The hardest thing we need to do here is infer types for later things.
  # This means we may need to disallow declaring new variables that aren't 
  # derived from old ones.
  #
  # Return a function

  # Example of getting type information. If we restrict the UDF to be "functional"
  # and only use the input types, we can do type inference for simple functions
  # using this.
  nvlHdr, names = nvl_header(ty, tree, tupargs)
  nvlBody = nvl_codegen(tree, names)

  def nvlFunc(*args):
    # Alternatively: nvlBody.toSeq or something
    return func(*args)

  setattr(nvlFunc, "nvlBody", nvlBody)
  setattr(nvlFunc, "nvlHdr", nvlHdr)
  return nvlFunc

def make_nvl(ty, func, tupargs):
  """
  Extracts the type information from the annotation, and then
  returns an NVL context object.
  """

  # This caches compiled NVL UDFs. These are currently stored as
  # functions, but it may make more sense to store something like
  # an NvlContext object, and then marshall it into something else
  # here so a caller can splice it into an existing NVL program.
  if func in registry:
    return registry[func]

  # If the NVL program doesn't exist, parse the type information
  assert isinstance(ty, basestring)

  ty = Parser().parse(ty)

  # Extract NVL from the Python function. This is where the AST parsing happens.
  nvlFunc = extract_nvl(ty, func, tupargs)

  # Cache the result.
  registry[func] = nvlFunc
  funcs[func.__name__] = nvlFunc.nvlHdr + " => " + nvlFunc.nvlBody
  return registry[func]

def nvl(ty, tupargs=False):
    return lambda f: make_nvl(ty, f, tupargs)
