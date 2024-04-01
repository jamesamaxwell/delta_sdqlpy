import sys
import ast
import os
from importlib.machinery import SourceFileLoader

from sdql_ir import *
from fast_dict_generator import *

import re

################################################################################

def mapping(datasetName):
    return datasetName + "_map"

def indent(text, indent='\t'):  # This function maybe should be put elsewhere
    lines = text.split('\n')
    indented_lines = [indent + line for line in lines]
    return '\n'.join(indented_lines)

class visitor(ast.NodeVisitor):
    code = ""
    func_name = None
    newVarCounter = 1
    isInJoinFilterLambda = False
    finalClosingPranthesis = ""
    varNames = []
    is_columnar_input = None
    is_assignment_sum = False
    dense_size = None

    def __init__(self, f_name, is_columnar_input):
        self.func_name = f_name
        self.is_columnar_input = is_columnar_input

    def visit_FunctionDef(self, node):
 
        for arg in node.args.args:
            if self.is_columnar_input:
                self.code += arg.arg + " = VarExpr(\"" + mapping(arg.arg) + "\")\n"
            else:
                self.code += arg.arg + " = VarExpr(\"" + arg.arg + "\")\n"

        self.code += "\n\n" + node.name + "="

        for b in node.body:
            self.visit(b)

    def visit_Call(self, node):
        if type(node.func)==ast.Attribute and node.func.attr=="sum":
            self.code += "SumBuilder("
            self.visit_Lambda(node.args[0])
            if (isinstance(node.func.value, ast.Call) and 
            isinstance(node.func.value.func, ast.Name) and 
            node.func.value.func.id == "getMap"):
                self.code += ", "
                self.visit(node.func.value)
                self.code += ", "
            elif type(node.func.value)==ast.Name:
                self.code += ", " + node.func.value.id + ", "
            elif (type(node.func.value)==ast.Call and type(node.func.value.func)==ast.Attribute):
                self.code += ", "
                self.visit_Call(node.func.value)
                self.code += ", "
            elif (type(node.func.value)==ast.Subscript):
                self.code += ", "
                self.visit_Subscript(node.func.value)
                self.code += ", "   
            elif (type(node.func.value)==ast.Attribute):
                self.code += ", "
                self.visit_Attribute(node.func.value)
                self.code += ", "   
            else:
                print ("Error: Unknown Sum Dict Type! Type: " + str(node.func.value))

            # if len(node.args)>1:
            if self.is_assignment_sum:
                self.code += "True"
                self.is_assignment_sum = False
            else:
                self.code += "False"
                # self.code += str(not node.args[1].value)

            if len(node.args)>1:
                if not (node.args[1].value):
                    self.code += ", \"phmap::flat_hash_map\""
            else:
                if self.dense_size != None:
                    self.code += ", \"dense_array(" + str(self.dense_size) + ")\""
                    self.dense_size = None
                
                # else:
                #     self.code += ", \"" + node.args[1].value + "\""
            
            self.code += ")"
            
        elif type(node.func)==ast.Name and node.func.id=="extractYear":
            self.code += "ExtFuncExpr(ExtFuncSymbol.ExtractYear, "
            self.visit(node.args[0])
            self.code += ", ConstantExpr(\"Nothing!\"), ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="startsWith":
            self.code += "ExtFuncExpr(ExtFuncSymbol.StartsWith, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="endsWith":
            self.code += "ExtFuncExpr(ExtFuncSymbol.EndsWith, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", ConstantExpr(\"Nothing!\"))"
        
        elif type(node.func)==ast.Name and node.func.id=="dictSize":
            self.code += "ExtFuncExpr(ExtFuncSymbol.DictSize, "
            self.visit(node.args[0])
            self.code += ", ConstantExpr(\"Nothing!\"), ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="substr":
            self.code += "ExtFuncExpr(ExtFuncSymbol.SubStr, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", "
            self.visit(node.args[2])
            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="firstIndex":
            self.code += "ExtFuncExpr(ExtFuncSymbol.FirstIndex, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="sr_dict":
            self.visit_Dict(node.args[0])

        elif type(node.func)==ast.Name and node.func.id=="record":
            self.code += "RecConsExpr(["
            
            counter = 0
            for key in node.args[0].keys:
                self.code += "(\""
                self.code += key.value
                self.code += "\", "
                self.visit(node.args[0].values[counter])
                self.code += "), "
                counter += 1
            self.code = self.code[:-2]

            self.code += "])"

        elif type(node.func)==ast.Name and node.func.id=="vector":
            self.code += "VecConsExpr([" 
            self.visit(node.args[0])
            self.code += "])"

        elif type(node.func)==ast.Attribute and node.func.attr=="concat":
            self.code += "ConcatExpr("
            self.visit(node.func.value)
            self.code += ", "
            self.visit(node.args[0])
            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="unique":
            self.is_assignment_sum = True
            self.visit(node.args[0])

        elif type(node.func)==ast.Name and node.func.id=="dense":
            self.dense_size = node.args[0].value
            self.visit(node.args[1])

        elif type(node.func)==ast.Attribute and node.func.attr=="joinBuild":
            self.code += "JoinPartitionBuilder("
            self.visit(node.func.value)
            self.code += ", \""
            self.code += node.args[0].value
            self.code += "\", "
            self.isInJoinFilterLambda = True
            self.visit(node.args[1])
            self.isInJoinFilterLambda = False
            self.code += ", ["
            
            if len(node.args[2].elts)>0:
                for el in node.args[2].elts:
                    self.code += "\"" + el.value + "\", "
                self.code = self.code[:-2] + "]"
            else:
                self.code += "]"

            if len(node.args)>3:
                if not (node.args[3].value):
                    self.code += ", \"phmap::flat_hash_map\""
                # else:
                #     self.code += ", \"" + node.args[3].value + "\""


            self.code += ")"

        elif type(node.func)==ast.Attribute and node.func.attr=="joinProbe":
            self.code += "JoinProbeBuilder("
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.func.value)
            self.code += ", \""
            self.code += node.args[1].value
            self.code += "\", "
            self.isInJoinFilterLambda = True
            self.visit(node.args[2])
            self.isInJoinFilterLambda = False
            self.code += ", "
            self.visit(node.args[3])

            if len(node.args)>4:
                self.code += ", "
                self.code += str(not node.args[4].value)

            if len(node.args)>5:
                self.code += ", \"" + ("phmap::parallel_flat_hash_map" if node.args[5].value else "phmap::flat_hash_map") + "\""

            self.code += ")"
        elif type(node.func)==ast.Name and node.func.id=="sdql_compile":
            pass

        elif type(node.func)==ast.Name and node.func.id=="updateMap":
            # print("UpdateMapBuilder:")
            self.code += "UpdateMapBuilder(M3MapBuilder(\""
            self.visit(node.args[0])
            self.code += "_map\")"
            
            self.code += ", "
            self.code += "MakeTupleBuilder(["
            if type(node.args[1]) == ast.Tuple:
                for i in range(0, len(node.args[1].elts) - 1):
                    self.visit(node.args[1].elts[i])
                    self.code += ", "
                self.visit(node.args[1].elts[len(node.args[1].elts) - 1])
            else:
                self.visit(node.args[1])

            self.code += "]), "
            self.visit(node.args[2])

            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="updateMapForEach":

            self.code += "ForEachUpdateMapBuilder(M3MapBuilder(\""
            self.visit(node.args[0])
            self.code += "_map\")"
            
            self.code += ", "
            self.code += "MakeTupleBuilder(["
            for i in range(0, len(node.args[1].elts) - 1):
                self.visit(node.args[1].elts[i])
                self.code += ", "

            self.visit(node.args[1].elts[len(node.args[1].elts) - 1])

            self.code += "]), "
            self.visit(node.args[2])

            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="getStream":
            # print("getStream")
            # print(node.args[0].value)
            parts = node.args[0].value.split('.', 1)
            if len(parts) > 1:
                self.code += "StreamParam("
                self.code += "\"" + parts[1] + "\""
                self.code += ")"
            else:
                self.code += "StreamParam("
                self.code += "\"" + node.args[0].value + "\""
                self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="getAgg":
            # print("\n\n\nget agg:\n" + node.args[0].id + "\n\n")
            self.code += "AggSumColumn("
            self.code += "\"" + node.args[0].id + "\", "
            self.code += "\"" + str(node.args[1].value) + "\")"

        elif type(node.func)==ast.Name and node.func.id=="getMap":
            self.code += "M3MapBuilder("
            self.code += "\"" + node.args[0].id + "_map\")"

        elif type(node.func)==ast.Name and node.func.id=="getValueOrDefault":
            self.code += "GVODBuilder(M3MapBuilder(\""
            self.visit(node.args[0])
            self.code += "_map\")"

            self.code += ", "
            self.code += "MakeTupleBuilder(["
            # print("GVOD tuple:")
            # print(node.args[1])
            if type(node.args[1]) == ast.Tuple:
                for i in range(0, len(node.args[1].elts) - 1):
                    self.visit(node.args[1].elts[i])
                    self.code += ", "
                self.visit(node.args[1].elts[len(node.args[1].elts) - 1])
            else:
                self.visit(node.args[1])

            # print(node.args[1])
            self.code += "]))"

        elif type(node.func)==ast.Name and node.func.id=="max":
            self.code += "Max(["
            for i in range(0, len(node.args[0].elts) - 1):
                self.visit(node.args[0].elts[i])
                self.code += ", "

            self.visit(node.args[0].elts[len(node.args[0].elts) - 1])
            # self.visit(node.args[0])
            self.code += "])"

        elif type(node.func)==ast.Name and node.func.id=="regex_match":
            self.code += "RegexMatch(ConstantExpr(\"" + node.args[0].value + "\"), "
            self.visit(node.args[1])
            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="dictTranspose":
            # print("\n\n\n\narg type: name")
            self.code += "DictTranspose(\"" 
            self.visit(node.args[0])
            self.code += "\")"

        elif type(node.func)==ast.Name and node.func.id=="reset_map":
            self.code += "ResetMap(\"" + node.args[0].id + "\")"

        elif type(node.func)==ast.Name:
            # print("\n\nAgg Call:\n")
            # print(node.func.id)
            # print("\n\n")
            self.code += "AggCall(\"" + node.func.id
            self.code += "\", "
            if len(node.args) > 0:
                self.code += "["
                for i in range(0, len(node.args) - 1):
                    self.visit(node.args[i])
                    self.code += ", "
                self.visit(node.args[len(node.args) - 1])
                self.code += "]"
            else:
                self.code += "None"
            self.code += ")"

        else:
            # print(node.func)
            print ("Error: Unknown Call Node!: " + str(node.func.id))
    
    def visit_Lambda(self, node):
        self.code += "lambda "
        for arg in node.args.args:
            self.code += arg.arg + ", "
        self.code = self.code[:-2] + ": "
        self.visit(node.body)
    
    def visit_IfExp(self, node):
        self.code += "IfExpr("
        self.visit(node.test)
        self.code += ", "
        self.visit(node.body)
        self.code += ", "
        # self.code += ", ConstantExpr(0)"
        self.visit(node.orelse)
        self.code += ")"

    def visit_Compare(self, node):
        if type(node.ops[0])==ast.In:
            self.code += "ExtFuncExpr(ExtFuncSymbol.StringContains, " 
            self.visit(node.left)
            self.code += ", ConstantExpr(-1), "
            self.visit(node.comparators[0])
            self.code += ") == ConstantExpr(True)"
            return
        self.code += "("
        self.visit(node.left)
        if type(node.ops[0])==ast.LtE:
            self.code += " <= "
        elif type(node.ops[0])==ast.GtE:
            self.code += " >= "
        elif type(node.ops[0])==ast.Lt:
            self.code += " < "
        elif type(node.ops[0])==ast.Gt:
            self.code += " > "
        elif type(node.ops[0])==ast.Eq:
            self.code += " == "
        elif type(node.ops[0])==ast.NotEq:

            self.code += " != "
        else:
            print ("Error: Unknown Compare Node! | " + str(type(node.ops[0])))
        self.visit(node.comparators[0])
        self.code += ")"

    def visit_Constant(self, node):
        if type(node.value) == str:
            self.code += "ConstantExpr(\"" + node.value + "\")"
        else:
            self.code += "ConstantExpr(" + str(node.value) + ")"

    def visit_UnaryOp(self, node):
        if type(node.op) == ast.USub:
            self.code += "ConstantExpr(-1)"
        else:
            print("Unary not defined!")
            return
        self.code += "*("
        self.visit(node.operand)
        self.code += ")" 

    def visit_BoolOp(self, node):
        op = None
        if type(node.op)==ast.And:
            op = " * "
        elif type(node.op)==ast.Or:
            op = " + "
        else:
            print ("Error: Unknown BoolOp Node!")
        self.code += "("
        for val in node.values:
            self.code += "("
            self.visit(val)
            self.code += ")"
            self.code += op
        self.code = self.code[:-3]
        self.code += ")"

    def visit_BinOp(self, node):
        # self.code += "("
        # self.visit(node.left)
        if type(node.op)==ast.Mult:
            self.code += "MulExpr("

        elif type(node.op)==ast.Sub:
            self.code += "SubExpr("

        elif type(node.op)==ast.Add:
            self.code += "AddExpr("

        elif type(node.op)==ast.Div:
            self.code += "DivExpr("

        else:
            print ("Error: Unknown BinOp Node!")

        self.visit(node.left)
        self.code += ", "
        self.visit(node.right)
        self.code += ")"

    def visit_Name(self, node):
        self.code += node.id

    def visit_Subscript(self, node):
        if self.isInJoinFilterLambda:
            if type(node.value) == ast.Subscript:
                self.code += "("
                self.visit_Subscript(node.value)
                self.code += ")["
                self.visit(node.slice.value)
                self.code += "]"
            elif type(node.slice.value) in [ast.Call, ast.Attribute]:
                self.code += node.value.id + "["
                self.visit(node.slice.value)
                self.code += "]"
            else:
                self.code += node.value.id
            return
        elif type(node.slice.value) == ast.Constant:
            self.code += node.value.id + "[" + str(node.slice.value.value) + "]"
        else:
            self.code += node.value.id + "["
            self.visit(node.slice.value)
            self.code += "]"


    def visit_Attribute(self, node):
        self.visit(node.value)
        self.code += "." + node.attr

    def visit_Return(self, node):
        self.code += "LetExpr(VarExpr(\"out\"), "
        self.visit(node.value)
        self.code += ", ConstantExpr(True))"

    def visit_Expr(self, node):
        self.visit(node.value)

    def visit_Dict(self, node):
        self.code += "DicConsExpr([("
        self.visit(node.keys[0])
        self.code += ", "
        self.visit(node.values[0])
        self.code += ")])"

    def visit_Assign(self, node):
        self.varNames.append(node.targets[0].id)
        self.code += "LetExpr(" + node.targets[0].id + ", "
        self.visit(node.value)
        self.code += ", "
        self.finalClosingPranthesis += ")"

    def visit_AugAssign(self, node):
        #print("AugAssign:")
        #print(node.op)
        self.varNames.append(node.target.id)
        self.code += "AugAssign("
        self.code += node.target.id
        self.code += ", "
        if type(node.op)==ast.Add:
            self.code += "\"+\""
        elif type(node.op)==ast.Add:
            self.code += "\"-\""
        else:
            print("Error: AugAssign op not recognised")
        
        self.code += ", "
        self.visit(node.value)
        self.code += ", "
        self.finalClosingPranthesis += ")"

    def visit_Index(self, node):
        self.visit(node.value)

    def visit(self, node):
        ast.NodeVisitor.visit(self, node)

    def generic_visit(self, node):
        print (">>> Generic Visit: " + type(node).__name__)
        print(node.elts[0].args[0].id)
        print(node.elts[0].args[1])
        print(node.elts[1])
        ast.NodeVisitor.generic_visit(self, node)


class searchVisitor(ast.NodeVisitor):
    condition = None
    foundNode = None

    def findNode(self, rootNode, conditionFunc):
        self.condition = conditionFunc
        self.visit(rootNode)
        return self.foundNode

    def generic_visit(self, node):
        if self.condition(node):
            self.foundNode = node
            return
        else:
            ast.NodeVisitor.generic_visit(self, node)

class searchVisitor2(ast.NodeVisitor):
    condition = None
    results = []

    def findNode(self, rootNode, conditionFunc):
        self.condition = conditionFunc
        self.visit(rootNode)
        return self.results

    def generic_visit(self, node):
        if self.condition(node):
            self.results.append(node)
            return
        else:
            ast.NodeVisitor.generic_visit(self, node)

def GenerateIRFile(fileName, code, path=""):
    print("SDQL_IR Generated @: " + os.path.join(os.path.dirname(__file__), path) + fileName + ".py")
    with open(os.path.join(os.path.dirname(__file__), path) + fileName + ".py", "w") as myfile:
        myfile.write(code)
        myfile.close()
    # print(">>> " + str(fileName) + ": IR code generated.")

def RemoveFile(path):
    print("File deleted: " + path)
    os.remove(path)

def CreateCPPFile(code, name, path):
    with open(os.path.join(os.path.dirname(__file__), path) + name + "_compiled.cpp", "w") as myfile:
        myfile.write(code)
        myfile.close()
    print(">>> " + str(name) + ": C++ code generated.")

def CreateSetupFile(code, name, path):
    with open(os.path.join(os.path.dirname(__file__), path) + name + "_compiled_setup.py", "w") as myfile:
        myfile.write(code)
        myfile.close()
    print(">>> setup.py file generated.")

################################################################################
    
def implementTriggers(functions, file_name, cores):
    cppcode = ""
    for function in functions:
        f_name = function.name
        print(">>>>>> Processing Trigger Function:", f_name)
        f_decorators = function.decorator_list
        decorator_node = f_decorators[0]
        entryType = None
        #print(decorator_node.args[1].id)
        if type(decorator_node.args[1]) == ast.Name:
            entryType = decorator_node.args[1].id

        # sdql_decorator_node_in_type = sdql_decorator_node.args[0]
           
        searchVis = searchVisitor()

        vis = visitor(f_name, False)
        vis.varNames = []
        apf = ast.parse(function)
        #print(apf)
        vis.visit(apf)
        vis.code += vis.finalClosingPranthesis
        vis.code += "\n\n\n" + f_name + "_typecache = {}\n"

    
        topCode = ""
        topCode += "import os\nimport sys\n"
        # topCode += "sys.path.append(\"" + file_dir + "lib/\")\n"
        topCode += "from sdql import *\n"
        topCode += "from sdql_ir import *\n"
        topCode += "from sdql_ir_type_inference import *\n"
        topCode += "import sdql_ir_cpp_generator_par as gen\n\n\n"

        varDeclaration = ""
        for var in vis.varNames:
            varDeclaration += var + " = VarExpr(\"" + var + "\")\n"
        
        vis.code = topCode + varDeclaration + "\n\n" + vis.code

        counter = 0
        #input_types_dict = "{"

        #input_types_dict = input_types_dict[:-2] + "}"
        #print("input types dict:")
        #print(input_types_dict)
        #vis.code += "infer_type(" + f_name + ", " + input_types_dict + ", " + f_name + "_typecache)\n"

        #GenerateIRFile(f_name, vis.code, os.path.dirname(os.path.abspath(file_name))+"/")
        #mod = SourceFileLoader(f_name, os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py").load_module()

        vis.code += "def get_code(db_code):\n\treturn gen.GenerateCPPCode(" + f_name + ", " + f_name + "_typecache, True, " + str(cores) + ")"

        GenerateIRFile(f_name, vis.code, os.path.dirname(os.path.abspath(file_name))+"/")
        mod = SourceFileLoader(f_name, os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py").load_module()
        print(os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py")
        RemoveFile(os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py")

        if entryType == None:
            cppcode += "void " + f_name + "(bool ent)\n{\n"
        else:
            cppcode += "void " + f_name + "(" + entryType + " ent)\n{\n"
        
        db_code = ""
            
        cppcode += mod.get_code(db_code) + "\n"
        cppcode += "}\n\n"

        
    #print(cppcode)
    return cppcode

def main():
    file_name = ""
    is_columnar_input = True
    is_fast_dict_input = False
    cores = 1
    file_dir = ""
    all_functions_in_types = {}
    all_functions_out_types = {}
    cppcode_first = ""

    if len(sys.argv) == 5:
        file_name = sys.argv[1]
        is_columnar_input = False if sys.argv[2]=="0" else True
        cores = sys.argv[3]
        file_dir = sys.argv[4]
        print ('>>> Processing File:', file_name)
    else:
        print ('>>> Compiler command parameters are not correct!')
    
    with open(file_name) as file:
        node = ast.parse(file.read())

    # c = 0
    # print("Node info:")
    # print(node)
    # for n in node.body:
    #     print("Node " + str(c) + ":")
    #     print(n)
    #     c += 1
    
    #print("----------------------------")

    searchVis = searchVisitor()

    # Creating C++ definitions of the data streams and maps
    for n in node.body:
        if isinstance(n, ast.FunctionDef) and len(n.decorator_list)>0 and n.decorator_list[0].func.id=="event_processor":
            event_types_name = n.decorator_list[0].args[0].values[0]
            map_types_name = n.decorator_list[0].args[0].values[1]
            #print("event types:")
            #print(event_types_name)
    
    event_types_node = None
    event_types_node = searchVis.findNode(node, lambda n:
        type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == event_types_name.id).value
    
    map_types_node = None
    map_types_node = searchVis.findNode(node, lambda n:
        type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == map_types_name.id).value
    
    entryDefs = "\n\n////////////////////////////////////\n\n"
    pse_entry_code = "\n"

    trigger_functions = [
    n for n in node.body 
    if isinstance(n, ast.FunctionDef) and len(n.decorator_list)>0 and n.decorator_list[0].func.id=="m3_trigger"
    ]

    for i in range(0, len(event_types_node.values)):
        entryDefs += "\nstruct " + event_types_node.values[i].id + " {\n"
        event_type = event_types_node.values[i]
        event_type_node = None
        event_type_node = searchVis.findNode(node, lambda n:
            type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == event_type.id).value
        pse_entry_code += "\nif (!strcmp(event_type, \"" + event_types_node.keys[i].value + "\")) {\n"
        entry_name = event_types_node.keys[i].value + "_entry"

        entryCodeBody = event_type.id + " " + entry_name + ";\n"
        structBody = ""
        
        for j in range(0, len(event_type_node.values)):
            structBody += event_type_node.values[j].value + " " + event_type_node.keys[j].value + ";\n"
            if (len(event_type_node.values[j].value) > 7) and (event_type_node.values[j].value[:7] == "VarChar"):
                entryCodeBody += "initializeVarCharFromPyObject(" + entry_name + "." + event_type_node.keys[j].value + ", PyList_GetItem(event_params, " + str(j) + "));\n"
            elif (event_type_node.values[j].value == "long"):
                entryCodeBody += entry_name + "." + event_type_node.keys[j].value + " = PyLong_AsLong(PyList_GetItem(event_params, " + str(j) + "));\n"
            elif (event_type_node.values[j].value == "double"):
                entryCodeBody += entry_name + "." + event_type_node.keys[j].value + " = PyFloat_AsDouble(PyList_GetItem(event_params, " + str(j) + "));\n"
            else:
                print("Unknown Event Data Type")

        for k in ["insert", "delete"]:
            entryCodeBody += "if (!strcmp(ins_or_del, \"" + k + "\")) {\n"
            triggerCallBody = ""
            triggerTypeLen = len(k) + len(event_types_node.keys[i].value) + 2

            for t in trigger_functions:
                if t.name[:(triggerTypeLen)] == k + "_" + event_types_node.keys[i].value + "_":
                    triggerCallBody += t.name + "(" + entry_name + ");\n"

            entryCodeBody += indent(triggerCallBody)
            entryCodeBody += "\n}\nelse "

        on_sys_ready_code = ""
        for t in trigger_functions:
            if t.name[:12] == "system_ready":
                on_sys_ready_code += t.name + "(true);\n"

        entryCodeBody += "{PyErr_SetString(PyExc_ValueError,\"Invalid trigger type.\");}"
        
        entryDefs += indent(structBody)
        entryDefs += "\n};\n"
        pse_entry_code += indent(entryCodeBody)
        pse_entry_code += "\n} else "

    pse_entry_code += """{
	PyErr_SetString(PyExc_ValueError,"Invalid event type.");
	return NULL;
}
Py_RETURN_NONE;
"""
    #print(entryDefs) # Data stream definitions
    #print("PSE code:")
    #print(pse_entry_code) # Process Stream Entry code
    osrfn = "\nstatic PyObject * on_system_ready(PyObject *self, PyObject* args)\n{\n"
    osrfn += indent(on_sys_ready_code)
    osrfn += "Py_RETURN_NONE;\n}\n"


    mapDefs = "\n\n////////////////////////////////////\n\n"

    m3maps = []

    for m in map_types_node.elts:
        m3maps.append(m.id)
        mapDefs += "static phmap::flat_hash_map<tuple<"
        map_type_node = None
        map_type_node = searchVis.findNode(node, lambda n:
            type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == m.id).value
        map_key = map_type_node.keys[0]
        map_type = map_type_node.values[0].value
        for i in range(0, len(map_key.args[0].values) - 1):
            mapDefs += map_key.args[0].values[i].value + ", "
        mapDefs += map_key.args[0].values[len(map_key.args[0].values) - 1].value
        mapDefs += ">, " + map_type + "> " + m.id + "_map;\n"

    #print(mapDefs) # Map definitions

    # Finding return type definition
    fastdictReturn = "\n\n////////////////////////////////////\n\n"
    fastdictReturn += "typedef struct {\n"
    
    for n in node.body:
        if isinstance(n, ast.FunctionDef) and len(n.decorator_list)>0 and n.decorator_list[0].func.id=="final_result":
            result_types_name = n.decorator_list[0].args[0]

    result_types_node = None
    result_types_node = searchVis.findNode(node, lambda n:
        type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == result_types_name.id).value
    
    fastdictReturn += "\tPyObject_HEAD\n\t"
    result_type = "phmap::flat_hash_map<tuple<"

    result_key = result_types_node.keys[0]
    return_maps = result_key.args[0].keys
    return_map_types = result_key.args[0].values

    for i in range(0, len(return_map_types) - 1):
        result_type += return_map_types[i].value + ", "
    result_type += return_map_types[len(return_map_types) - 1].value
    result_type += ">, bool>"
    fastdictReturn += result_type
    fastdictReturn += "* dict;\n"
    fastdictReturn += "} FastDict_return;\n"

    #print(fastdictReturn) # Return type definition

    # Helper Functions:

    helperFuncs = "\n\n////////////////////////////////////\n\n"
    helperFuncs += """

double arrayMax(std::initializer_list<double> list) {
    if (list.size() == 0) {
        return std::numeric_limits<double>::quiet_NaN(); // Return NaN if the list is empty
    }
    
    double max = *list.begin(); // Initialize max with the first element of the list
    for (double num : list) {
        if (num > max) {
            max = num; // Update max if a larger value is found
        }
    }
    return max; // Return the highest number found
}

long arrayMax(std::initializer_list<long> list) {
    if (list.size() == 0) {
        return std::numeric_limits<long>::min(); // Return min value if the list is empty
    }
    
    long max = *list.begin(); // Initialize max with the first element of the list
    for (long num : list) {
        if (num > max) {
            max = num; // Update max if a larger value is found
        }
    }
    return max; // Return the highest number found
}

double arrayMin(std::initializer_list<double> list) {
    if (list.size() == 0) {
        return std::numeric_limits<double>::quiet_NaN(); // Return NaN if the list is empty
    }
    
    double min = *list.begin(); // Initialize min with the first element of the list
    for (double num : list) {
        if (num < min) {
            min = num; // Update min if a smaller value is found
        }
    }
    return min; // Return the smallest number found
}

long arrayMin(std::initializer_list<long> list) {
    if (list.size() == 0) {
        return std::numeric_limits<long>::max(); // Return max value if the list is empty, as we're looking for the min
    }
    
    long min = *list.begin(); // Initialize min with the first element of the list
    for (long num : list) {
        if (num < min) {
            min = num; // Update min if a smaller value is found
        }
    }
    return min; // Return the smallest number found
}

template<unsigned maxLen>
void initializeVarCharFromPyObject(VarChar<maxLen>& varChar, PyObject* pyObject) {
    if (!PyUnicode_Check(pyObject)) {
        std::wcerr << L"Provided PyObject is not a Unicode string." << std::endl;
        return;
    }

    // Determine the length of the Python string to allocate enough space
    Py_ssize_t size = PyUnicode_GetLength(pyObject) + 1; // +1 for null terminator
    wchar_t* buffer = new wchar_t[size];
    
    // Convert Python string to wide character string
    PyUnicode_AsWideChar(pyObject, buffer, size);

    // Initialize or assign the first character, ensuring not to exceed maxLen
    if (maxLen > 0) {
        varChar = VarChar<maxLen>(buffer);
    }

    delete[] buffer; // Clean up allocated memory
}

/*
template<typename MapType, typename TupleType, typename ValueType>
void updateMap(MapType& map, const TupleType& key, ValueType&& value) {
    auto it = map.find(key);
    if (it != map.end()) {
        it->second += std::forward<ValueType>(value); // Update existing value
    } else {
        map[key] = std::forward<ValueType>(value); // Insert new key with value
    }
}
*/

template<typename MapType, typename TupleType>
int updateMap(MapType& map, const TupleType& key, long value) {
    if (value == 0) {
        // Do not update the map if the value is 0
        return -1; // Indicate no operation was performed due to zero value
    }

    auto it = map.find(key);
    if (it != map.end()) {
        it->second += value; // Update existing value
        return 0; // No new key was created
    } else {
        map[key] = value; // Insert new key with value
        return 1; // A new key was created
    }
}

template<typename MapType, typename TupleType>
int updateMap(MapType& map, const TupleType& key, double value) {
    if (value == 0.0) {
        // Do not update the map if the value is 0.0
        return -1; // Indicate no operation was performed due to zero value
    }

    auto it = map.find(key);
    if (it != map.end()) {
        it->second += value; // Update existing value
        return 0; // No new key was created
    } else {
        map[key] = value; // Insert new key with value
        return 1; // A new key was created
    }
}

template<typename KeyType>
double getValueOrDefault(const phmap::flat_hash_map<KeyType, double>& map, const KeyType& key) {
    auto it = map.find(key);
    if (it != map.end()) {
        return it->second; // Found the key, return the associated value.
    } else {
        return 0.0; // Key not found, return a default double value.
    }
}

template<typename KeyType>
long getValueOrDefault(const phmap::flat_hash_map<KeyType, long>& map, const KeyType& key) {
    auto it = map.find(key);
    if (it != map.end()) {
        return it->second; // Found the key, return the associated value.
    } else {
        return 0; // Key not found, return a default long value.
    }
}

std::wstring stringToWString(const std::string& str) {
    if(str.empty()) return std::wstring();
    std::wstring wstr(str.size(), L' '); // Allocate space for wide characters
    std::mbstowcs(&wstr[0], str.c_str(), str.size()); // Convert multibyte to wide characters
    return wstr;
}

// Function to match regex
template<unsigned maxLen>
bool matchRegex(const wchar_t* pattern, const VarChar<maxLen>& str) {
    // Convert std::string pattern to std::wstring
    // std::wstring wpattern = stringToWString(pattern);
    std::wregex regexPattern(pattern);
    
    // Convert VarChar to std::wstring for regex operations
    std::wstring wstr(str.data); // Directly constructing std::wstring from wchar_t array
    
    // Create a wide string regex object using the pattern
    // std::wregex regexPattern(wpattern);
    
    // Check if the string matches the pattern
    bool isMatch = std::regex_match(wstr, regexPattern);
    
    return isMatch;
}

// Simplified getInd without bounds checking
template<typename T>
T getInd(const T& value, double) { // May need to write a seperate version of this for VarChar
    // For long, double, bool - return as is.
    return value;
}

template<typename T>
T getInd(const std::vector<T>& vec, double x) {
    // Directly convert x to size_t for indexing without bounds checking
    size_t index = static_cast<size_t>(x);
    return vec[index];
}

// Helper to apply getInd to each element of a tuple
template<typename Func, typename... Ts, std::size_t... Is>
auto transformTupleImpl(const std::tuple<Ts...>& t, Func func, double x, std::index_sequence<Is...>) {
    return std::make_tuple(func(std::get<Is>(t), x)...);
}

template<typename... Ts>
auto transformTuple(const std::tuple<Ts...>& t, double x) {
    return transformTupleImpl(t, getInd<Ts>..., x, std::make_index_sequence<sizeof...(Ts)>());
}

/*
template<typename MapType, typename TupleType, typename ValueType>
void updateMapForEach(MapType& map, TupleType& t, ValueType v) { // In the long run this is probably not optimal
    n = std::tuple_size<decltype(t)>::value;
    for (int i = 0; i < n; i++) {
        t1 = transformTuple(t, i);
        v1 = getInd(v, i);
        updateMap(map, t1, v1);
    }
}
*/

template<std::size_t I = 0, typename FuncT, typename... Tp, typename... Vp>
inline typename std::enable_if<I == sizeof...(Tp), void>::type
for_each_in_tuple(std::tuple<Tp...>&, FuncT, std::tuple<Vp...>&) {}

template<std::size_t I = 0, typename FuncT, typename... Tp, typename... Vp>
inline typename std::enable_if<I < sizeof...(Tp), void>::type
for_each_in_tuple(std::tuple<Tp...>& t, FuncT f, std::tuple<Vp...>& vectors) {
    f(std::get<I>(vectors), std::get<I>(t));
    for_each_in_tuple<I + 1, FuncT, Tp..., Vp...>(t, f, vectors);
}

// Function to transpose the dictionary
template<typename MapType>
auto transpose_dict(const MapType& inputMap) {
    using KeyTupleType = typename MapType::key_type;
    constexpr auto tupleSize = std::tuple_size<KeyTupleType>::value;

    // Creating a tuple of vectors for each type in the key tuple
    auto vectors = std::apply([](auto&&... args) {
        return std::make_tuple(std::vector<decltype(args)>{}...);
    }, KeyTupleType{});

    std::vector<typename MapType::mapped_type> values;

    for (const auto& [key, value] : inputMap) {
        for_each_in_tuple(key, [](auto& vec, auto& val) {
            vec.push_back(val);
        }, vectors);
        values.push_back(value);
    }

    return std::make_pair(vectors, values);
}

template<typename MapType>
int reset_map(MapType& map) {
    int numEntriesDeleted = map.size(); // Get the number of entries before clearing
    map.clear(); // Clear the map, removing all entries
    return numEntriesDeleted; // Return the number of entries that were removed
}

"""
   # print(helperFuncs)

    functions = [
        n for n in node.body 
        if isinstance(n, ast.FunctionDef) and len(n.decorator_list)>0 and n.decorator_list[0].func.id=="agg_sum"
        ]

    cppcode = """
#include <Python.h>
#include "numpy/arrayobject.h"
#include \"""" + file_dir + """include/headers.h"
#include <tuple>
#include <utility>
#include <initializer_list>
#include <limits>
#include <regex>
//#include <string>
//#include <wchar.h>
"""

    cppcode_first = cppcode
    cppcode = ""

    cppcode += """
int init_numpy(){import_array();return 0;}

/*
static string GetType(PyObject *obj)
{
    PyTypeObject* type = obj->ob_type;
    const char* p = type->tp_name;
    return string(p);
}
*/

static wchar_t* ConstantString(const char* data, int len)
{
    wchar_t* wc = new wchar_t[len]; 
    mbstowcs (wc, data, len); 
    return wc;
} 

using namespace std;

tbb::task_scheduler_init scheduler(""" + cores + """);

"""

    if is_columnar_input:
        cppcode += """

class DB
{
    public:
"""
    
    searchViss = searchVisitor2()
    read_csv_nodes = searchViss.findNode(node, 
        lambda n: type(n) == ast.Assign and type(n.value) == ast.Call and type(n.value.func) == ast.Name and n.value.func.id == "read_csv"
    )

    for r in read_csv_nodes:
        dataset_name = r.value.args[2].value
        cppcode += "\t\tint " + dataset_name + "_dataset_size = 0;\n"

    cppcode += """};

"""
    inits = cppcode
    inits += helperFuncs
    aggCode = "////////////////////////////////////\n\n"
    
    for function in functions:
        f_name = function.name
        print(">>>>>> Processing Function:", f_name)
        f_decorators = function.decorator_list
        sdql_decorator_node = f_decorators[0]
        sdql_decorator_node_in_type = sdql_decorator_node.args[0]
        cppParams = {}
        function_params = []
        for arg in function.args.args:
            function_params.append(arg.arg)
            cppParams[arg.arg] = ""
        searchVis = searchVisitor()
        entryType = "bool"

        if type(sdql_decorator_node.args[1]) != ast.Constant:
            entryType = sdql_decorator_node.args[1].id

        if sdql_decorator_node.args[2].id == "int":
            agg_return_type = "long"
        elif sdql_decorator_node.args[2].id == "float":
            agg_return_type = "double"
        else:
            print("\nerror: invalid return type: " + sdql_decorator_node.args[2].id)

        vis = visitor(f_name, is_columnar_input)
        vis.varNames = []
        apf = ast.parse(function)
        #print(apf)
        vis.visit(apf)
        vis.code += vis.finalClosingPranthesis
        vis.code += "\n\n\n" + f_name + "_typecache = {}\n"

    
        topCode = ""
        topCode += "import os\nimport sys\n"
        # topCode += "sys.path.append(\"" + file_dir + "lib/\")\n"
        topCode += "from sdql import *\n"
        topCode += "from sdql_ir import *\n"
        topCode += "from sdql_ir_type_inference import *\n"
        topCode += "import sdql_ir_cpp_generator_par as gen\n\n\n"

        varDeclaration = ""
        for var in vis.varNames:
            varDeclaration += var + " = VarExpr(\"" + var + "\")\n"
        
        vis.code = topCode + varDeclaration + "\n\n" + vis.code

        counter = 0
        input_types_dict = "{"

        for type_var_name in sdql_decorator_node_in_type.values:
            # print(sdql_decorator_node_in_type.keys[counter].value)
            var_name_key = sdql_decorator_node_in_type.keys[counter].value

            #print("\n\ntype var name:\n\n" + str(type(type_var_name)) + "\n\n\n\n")
            in_type_node = None
            if type(type_var_name) == ast.Name:
                in_type_node = searchVis.findNode(node, lambda n:
                        type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value
            else:
                in_type_node = type_var_name

            if is_columnar_input:
                input_types_dict += "\"" + mapping(var_name_key) + "\": "
            else:
                input_types_dict += "\"" + var_name_key + "\": "
            if type(in_type_node) == ast.Dict:
                input_types_dict += "DictionaryType(RecordType(["
                if type(in_type_node.keys[0]) == ast.Call:
                    recordKeys = in_type_node.keys[0].args[0].keys
                    recordVals = in_type_node.keys[0].args[0].values
                elif type(in_type_node.keys[0]) == ast.Constant:
                    recordKeys = in_type_node.keys
                    recordVals = in_type_node.values
                for i in range(0 , len(recordKeys)):
                    input_types_dict += "(\"" + recordKeys[i].value + "\", "
                    if type(recordVals[i]) == ast.Call and recordVals[i].func.id == "string":
                        input_types_dict += "StringType(" + str(recordVals[i].args[0].value if len(recordVals[i].args) else 25) + ")"
                    elif recordVals[i].value == "date":
                        input_types_dict += "IntType()"
                    elif recordVals[i].value == "long":
                        input_types_dict += "IntType()"
                    elif recordVals[i].value == "double":
                        input_types_dict += "FloatType()"
                    elif recordVals[i].value[:7] == "VarChar":
                        number = re.search(r'VarChar<(.*)>', recordVals[i].value).group(1)
                        input_types_dict += "StringType(" + number + ")"
                    # input_types_dict += recordVals[i].value
                    input_types_dict += "), " 
                input_types_dict = input_types_dict[:-2]
                input_types_dict += "]), "
                dict_val_type = in_type_node.values[0]
                # print(dict_val_type.value)
                if dict_val_type.value == "long":
                    input_types_dict += "IntType()"
                elif dict_val_type.value == "double":
                    input_types_dict += "FloatType()"
                else:
                    # print("\n\n\nBoolType map:\n\n\n")
                    input_types_dict += "BoolType()"
                input_types_dict += "), "
                
            elif type(in_type_node) == ast.Tuple:
                if var_name_key in function_params:
                    cppParams[var_name_key] += "std::pair<tuple<"
                input_types_dict += "TupleType(RecordType(["
                recordKeys = in_type_node.elts[0].args[0].keys
                recordVals = in_type_node.elts[0].args[0].values
                for i in range(0 , len(recordKeys)):
                    input_types_dict += "(\"" + recordKeys[i].value + "\", "
                    if type(recordVals[i]) == ast.Call and recordVals[i].func.id == "string":
                        input_types_dict += "StringType(" + str(recordVals[i].args[0].value if len(recordVals[i].args) else 25) + ")"
                    elif recordVals[i].value == "date":
                        input_types_dict += "IntType()"
                    elif recordVals[i].value == "long":
                        input_types_dict += "IntType()"
                    elif recordVals[i].value == "double":
                        input_types_dict += "FloatType()"
                    elif recordVals[i].value[:7] == "VarChar":
                        number = re.search(r'VarChar<(.*)>', recordVals[i].value).group(1)
                        input_types_dict += "StringType(" + number + ")"
                    # input_types_dict += recordVals[i].value
                    input_types_dict += "), "
                    if var_name_key in function_params:
                        cppParams[var_name_key] += recordVals[i].value
                        cppParams[var_name_key] += ", "
                input_types_dict = input_types_dict[:-2]
                if in_type_node.elts[1].value == "long":
                    map_return_type = "IntType()"
                elif in_type_node.elts[1].value == "bool":
                    map_return_type = "BoolType()"
                else:
                    print("\nunrecognised tuple 2nd element type in aggsum params\n")

                if var_name_key in function_params:
                    cppParams[var_name_key] = cppParams[var_name_key][:-2] + ">, "
                    cppParams[var_name_key] += in_type_node.elts[1].value + ">"
                input_types_dict += "]), " + map_return_type + "), "
            else:
                print("Error: in_type is unknown!")
                return

           
            counter += 1

        input_types_dict = input_types_dict[:-2] + "}"
        vis.code += "infer_type(" + f_name + ", " + input_types_dict + ", " + f_name + "_typecache)\n"
        # print(function_params)
        # print(cppParams)

        # GenerateIRFile(f_name, vis.code, os.path.dirname(os.path.abspath(file_name))+"/")
        # mod = SourceFileLoader(f_name, os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py").load_module()
        
        # all_functions_out_types[f_name] = (getattr(mod, f_name + "_typecache")[getattr(mod, "results").id])

        # out_type = all_functions_out_types[f_name]
        # if type(out_type) == IntType:
        #     out_type = "int"
        # elif type(out_type) == FloatType:
        #     out_type = "float"
        # elif type(out_type) == DictionaryType:
        #     out_type = "DictionaryType()"
        #     abbrOutType = getAbbrDictName(all_functions_out_types[f_name].fromType, BoolType())

        vis.code += "def get_code(db_code):\n\treturn gen.GenerateCPPCode(" + f_name + ", " + f_name + "_typecache, True, " + str(cores) + ")"

        GenerateIRFile(f_name, vis.code, os.path.dirname(os.path.abspath(file_name))+"/")
        mod = SourceFileLoader(f_name, os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py").load_module()
        RemoveFile(os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py")


        aggCode += agg_return_type + " " + f_name + "("
        aggCode += entryType + " ent, "

        counter = 0
        for a in function_params:
            if len(cppParams[a]) > 0:
                aggCode += cppParams[a] + " " + a + "_map, "
                counter += 1

        # if len(entryType) == 0 and len(function_params) == 0:
        #     aggCode += ")\n{"
        # else:
        aggCode = aggCode[:-2] + ")\n{"

        # print(aggCode)
        db_code = ""

        if is_columnar_input:
            db_code += "\tconst static int numpy_initialized =  init_numpy();\n\n\n"

        if is_fast_dict_input:
            db_code += "\n\n"

            counter = 0
            for type_var_name in sdql_decorator_node_in_type.values:        
                in_type_node = searchVis.findNode(node, lambda n:
                        type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value

                input_vals = in_type_node.keys[0].args[0].values
                
                abbr=""
                for v in input_vals:
                    if type(v) == ast.Call and v.func.id == "string":
                        abbr += "s" + str(v.args[0].value)
                    elif v.id == "date":
                        abbr += "i"   
                    elif v.id == "int":
                        abbr += "i"
                    elif v.id == "float":
                        abbr += "f"
                    elif v.id == "bool":
                        abbr += "b"
                    else:
                        print("Error: input type not defined!")
                abbr = "FastDict_" + abbr + "_b"
                db_code += "\tauto& " + sdql_decorator_node_in_type.keys[counter].value + " = *(((" + abbr + "*)PyDict_GetItemString(db_, \"" + sdql_decorator_node_in_type.keys[counter].value + "\"))->dict);\n"
                counter += 1

            db_code += "\n\n"
        else:
            if is_columnar_input:
                counter = 0
                for type_var_name in sdql_decorator_node_in_type.values:        
                    in_type_node = None
                    if type(type_var_name) == ast.Name:
                        in_type_node = searchVis.findNode(node, lambda n:
                                type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value
                    else:
                        in_type_node = type_var_name                    
                    db_code += "\tauto " + sdql_decorator_node_in_type.keys[counter].value + "_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, " + str(counter) + "), 0));\n"
                    db_code += "\t" + mapping(sdql_decorator_node_in_type.keys[counter].value) + "_size = " + sdql_decorator_node_in_type.keys[counter].value + "_size;\n"

                    if type(in_type_node) == ast.Dict:
                        if type(in_type_node.keys[0]) == ast.Call:
                            in_type_obj = in_type_node.keys[0].args[0]
                        elif type(in_type_node.keys[0]) == ast.Constant:
                            in_type_obj = in_type_node
                        else:
                            print("Unrecognised dict type")
                    elif type(in_type_node) == ast.Tuple:
                        in_type_obj = in_type_node.elts[0].args[0]

                    # print("\n\n\n\n\n\n")
                    # print(in_type_obj)
                    # print(in_type_obj.keys)
                    # print(in_type_obj.values)
                    # print("\n\n\n\n\n\n")
                    input_keys = in_type_obj.keys
                    input_vals = in_type_obj.values
                    counter2 = 0
                    for k in input_keys:

                        if type(input_vals[counter2]) == ast.Call and input_vals[counter2].func.id == "string":
                            db_code += "\tauto " + k.value + "= (VarChar<" + str(input_vals[counter2].args[0].value if len(input_vals[counter2].args) else 25) + ">*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, " + str(counter) + "), " + str(counter2) + "));\n"
                            counter2 += 1
                            continue

                        db_code += "\tauto " + k.value + " = ("
                        # if input_vals[counter2].id == "date":
                        #     db_code += "long*"   
                        # elif input_vals[counter2].id == "int":
                        #     db_code += "long*"
                        # elif input_vals[counter2].id == "float":
                        #     db_code += "double*"
                        # elif input_vals[counter2].id == "bool":
                        #     db_code += "bool*"
                        # else:
                        #     print("Error: input type not defined!")
                        db_code += input_vals[counter2].value + "*"
                        db_code += ")PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, " + str(counter) + "), " + str(counter2) + "));\n"
                        counter2 += 1           

                    db_code += "\n"
                    counter += 1
            else:
                counter = 0
                for type_var_name in sdql_decorator_node_in_type.values:        
                    if counter>0:
                        db_code += "\t"

                    db_code += "phmap::flat_hash_map<tuple<" 
                    
                    in_type_node = None
                    in_type_node = searchVis.findNode(node, lambda n: type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value
                    input_keys = in_type_node.keys[0].args[0].keys
                    input_vals = in_type_node.keys[0].args[0].values
                    counter2 = 0

                    for k in input_keys:
                        if type(input_vals[counter2]) == ast.Call and input_vals[counter2].func.id == "string":
                            db_code += "VarChar<" + str(input_vals[counter2].args[0].value if len(input_vals[counter2].args) else 25) + ">, "
                            counter2 += 1
                            continue
                        if input_vals[counter2].id == "date":
                            db_code += "long"   
                        elif input_vals[counter2].id == "int":
                            db_code += "long"
                        elif input_vals[counter2].id == "float":
                            db_code += "double"
                        elif input_vals[counter2].id == "bool":
                            db_code += "bool"
                        else:
                            print("Error: input type not defined!")
                        
                        db_code += ", "
                        counter2 += 1

                    db_code = db_code[:-2]
                    db_code += ">, bool> " + sdql_decorator_node_in_type.keys[counter].value + ";\n"
                    db_code += "\tauto " + sdql_decorator_node_in_type.keys[counter].value + "_py = PyDict_Keys(PyObject_GetAttr(PyDict_GetItemString(db_, \"" + sdql_decorator_node_in_type.keys[counter].value + "\"), PyUnicode_FromString(\"_sr_dict__container\")));"
                    
                    db_code += "\n\tauto " + sdql_decorator_node_in_type.keys[counter].value + "_py_size = PyList_Size(" + sdql_decorator_node_in_type.keys[counter].value + "_py" + ");"
                    db_code += "\n\tfor (int i=0; i< " + sdql_decorator_node_in_type.keys[counter].value + "_py_size; i++)\n\t{\n"
                    db_code += "\t\tauto tmpRec = PyObject_GetAttr(PyList_GetItem(" + sdql_decorator_node_in_type.keys[counter].value + "_py, i), PyUnicode_FromString(\"_sr_dict__container\"));\n"
                    db_code += "\t\tauto tmpRecKeys = PyDict_Keys(tmpRec);\n"
                    db_code += "\t\tauto tmpRecVals = PyDict_Values(tmpRec);\n"
                    db_code += "\n\t\tauto key_size = PyList_Size(tmpRecKeys);\n"

                    counter3 = 0              

                    db_code += "\t\t" + sdql_decorator_node_in_type.keys[counter].value + "[make_tuple("
                    
                    for val in input_vals:                
                        if type(val) == ast.Call and val.func.id == "string":
                            db_code += "VarChar<" + str(val.args[0].value if len(val.args) else 25) + ">(PyUnicode_AsWideCharString(PyList_GetItem(tmpRecVals, " + str(counter3) + "), NULL)), "
                            counter3 += 1
                            continue                
                        if val.id == "date":
                            db_code += "PyLong_AsLong"   
                        elif val.id == "int":
                            db_code += "PyLong_AsLong"
                        elif val.id == "float":
                            db_code += "PyFloat_AsDouble"
                        else:
                            print("Error: input type not defined!")
                    
                        db_code += "(PyList_GetItem(tmpRecVals, " + str(counter3) + ")), "
                        counter3+=1

                    db_code = db_code[:-2] + ")] = true;"

                    db_code += "\n\t}\n\n" 

                    counter += 1
                db_code += "\n\n"
            
        # print("db_code:")
        # print(db_code)
        # print("get_code:")
        # print(mod.get_code(db_code))
        aggCode += indent(mod.get_code(db_code) + "\nreturn out;")
        aggCode += "\n}\n\n"


    triggerCode = implementTriggers(trigger_functions, file_name, cores)
    # print("trigger code:")
    # print(triggerCode)

    processStreamEventCode = ""
    processStreamEventCode += """
static PyObject * process_stream_event(PyObject *self, PyObject* args) 
{
	const char * ins_or_del;
	const char * event_type;
	PyObject * event_params;

	if (!PyArg_ParseTuple(args, "ssO", &ins_or_del, &event_type, &event_params)) {
		PyErr_SetString(PyExc_ValueError,"Error while parsing the stream event.1");
		return NULL;
	}

"""

    processStreamEventCode += indent(pse_entry_code)
    processStreamEventCode += """
}

static PyObject * view_snapshot(PyObject *self, PyObject* args) 
{
"""

    pse_entry_code = ""
    pse_entry_code += "auto out = " + result_type + "({});\n\n"

    # print("m3maps:")
    # print(m3maps)

    for m in return_maps: # This could be improved probably
        if m.value in m3maps:
            pse_entry_code += "\nfor (auto temp : " + m.value + "_map) {\n\t"
            pse_entry_code += "out[tuple_cat(temp.first, make_tuple("
            for i in range(0, len(return_maps) - 1):
                if return_maps[i].value in m3maps:
                    pse_entry_code += "\n\t\tgetValueOrDefault(" + return_maps[i].value + "_map, temp.first),"

            pse_entry_code += "\n\t\tgetValueOrDefault(" + return_maps[len(return_maps) - 1].value + "_map, temp.first)"
            pse_entry_code += "\n\t\t))] = true;"
            pse_entry_code += "\n}\n"

    pse_entry_code += "\nFastDict_return* result = (FastDict_return*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString(\""
    pse_entry_code += file_name.split("/")[-1].split(".")[0]
    pse_entry_code += "_fastdict_compiled\")), (char*)\"new_FastDict_return\"), nullptr), nullptr);\n"
    pse_entry_code += "*(result->dict) = out;\n\n"
    pse_entry_code += "return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString(\"sdqlpy.fastd\")), \"fastd\"), Py_BuildValue(\"(OO)\", result, PyUnicode_FromString(\""
    pse_entry_code += file_name.split("/")[-1].split(".")[0]
    pse_entry_code += "\")));\n"

    processStreamEventCode += indent(pse_entry_code)
    processStreamEventCode += "\n}\n"
    processStreamEventCode += osrfn
    # print("process stream event code:")
    # print(processStreamEventCode)

    mod_name = file_name.split("/")[-1].split(".")[0] + "_compiled"
    
    module_defs = ""
    module_defs += """
static PyMethodDef """ + mod_name + """_methods[] = {\n"""
    
    cppcode += """
static PyMethodDef """ + mod_name + """_methods[] = {\n"""

    for f in functions:
        cppcode += "{\"" + f.name + "_compiled\", " + f.name + ", METH_VARARGS, \"\"},\n"

    cppcode += """
{"process_stream_event_compiled", process_stream_event, METH_VARARGS, ""},

{NULL,		NULL}		/* sentinel */
};

///////////////////////////////////////////////////////////////////////

static char module_docstring[] = "";

static struct PyModuleDef """ + mod_name + """ = 
{
    PyModuleDef_HEAD_INIT,
    """ + "\"" + mod_name + "\"" + """,
    module_docstring,
    -1,
    """ + mod_name + """_methods
};

PyMODINIT_FUNC PyInit_""" + mod_name + """(void) 
{
    return PyModule_Create(&""" + mod_name + """);
}

int main(int argc, char **argv)
{
	Py_SetProgramName((wchar_t*)argv[0]);
	Py_Initialize();
	PyInit_""" + mod_name + """();
	Py_Exit(0);
}"""
    
    module_defs += """
{"process_stream_event_compiled", process_stream_event, METH_VARARGS, ""},
{"view_snapshot_compiled", view_snapshot, METH_VARARGS, "Create and return the update map as a PyObject"},
{"on_system_ready_compiled", on_system_ready, METH_VARARGS, ""},

{NULL,		NULL}		/* sentinel */
};

///////////////////////////////////////////////////////////////////////

static char module_docstring[] = "";

static struct PyModuleDef """ + mod_name + """ = 
{
    PyModuleDef_HEAD_INIT,
    """ + "\"" + mod_name + "\"" + """,
    module_docstring,
    -1,
    """ + mod_name + """_methods
};

PyMODINIT_FUNC PyInit_""" + mod_name + """(void) 
{
    return PyModule_Create(&""" + mod_name + """);
}

int main(int argc, char **argv)
{
	Py_SetProgramName((wchar_t*)argv[0]);
	Py_Initialize();
	PyInit_""" + mod_name + """();
	Py_Exit(0);
}"""

    output_dicts = {}

    for k,v in all_functions_out_types.items():
        if type(v)==DictionaryType:
            output_dicts[k] = v


    abbrCache = []
    tmp = {}
    for k,v in output_dicts.items():
        tmp_v = v.fromType
        if getAbbrDictName(tmp_v, BoolType()) not in abbrCache:
            abbrCache.append(getAbbrDictName(tmp_v, BoolType()))
            tmp[k]=v

    dbtoaster_code = cppcode_first
    dbtoaster_code += entryDefs
    dbtoaster_code += mapDefs
    dbtoaster_code += fastdictReturn
    dbtoaster_code += inits
    dbtoaster_code += aggCode
    dbtoaster_code += triggerCode
    dbtoaster_code += processStreamEventCode
    dbtoaster_code += module_defs

    cppcode_first += getAllDictDefinitions(tmp) + "\n"
    cppcode_first += cppcode + "\n"
    cppcode = cppcode_first

    CreateCPPFile(dbtoaster_code, file_name[:-3], os.path.dirname(os.path.abspath(file_name))+"/")

    generate_fastdicts(tmp, file_name, file_dir) 
    #m3_fast_dict_code = "\n\n////////////////////////////////////////////////////////////////////\n\n"
    file_path = file_name
    cCode = """
#include <Python.h>
#include \"""" + file_dir + """include/headers.h"

using namespace std;

/*
static string GetType(PyObject *obj)
{
    PyTypeObject* type = obj->ob_type;
    const char* p = type->tp_name;
    return string(p);
}
*/
"""
    abbr = "FastDict_return"
    cpp = result_type
    # rec_keys = list(list(zip(*v.fromType.typesList))[0])
    rec_keys = [k.value for k in return_maps]
    # rec_vals = list(list(zip(*v.fromType.typesList))[1])
    rec_vals = [k.value for k in return_map_types]
    cCode += """
////////////////////////////////////////////////////////////////////

typedef struct {
    PyObject_HEAD
    """ + cpp + """* dict;
    vector<string> cols;
} """ + abbr + """;


static PyTypeObject """ + abbr + """_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    \"fast_dict.""" + abbr + """\"
    };

static PyMethodDef """ + abbr + """_methods[] = {
    {NULL, NULL}		/* sentinel */
};

static int """ + abbr + """_init(""" + abbr + """ *self, PyObject *args, PyObject *kwds)
// initialize """ + abbr + """ Object
{
    self->dict = new """ + cpp + """();
    """

    for col in rec_keys:
        cCode += "self->cols.push_back(\"" + col + "\");\n\t"

    cCode += """
    return 0;
}

static void """ + abbr + """_Type_init()
{
    """ + abbr + """_Type.tp_new = PyType_GenericNew;
    """ + abbr + """_Type.tp_basicsize=sizeof(""" + abbr + """);
    """ + abbr + """_Type.tp_flags=Py_TPFLAGS_DEFAULT;
    """ + abbr + """_Type.tp_methods=""" + abbr + """_methods;
    """ + abbr + """_Type.tp_init=(initproc)""" + abbr + """_init;
}

static PyObject * """ + abbr + """_Set(""" + abbr + """ *self, PyObject *pykey, PyObject *pyval)
{ 
    PyObject* pykey_data = PyDict_Values(PyObject_GetAttrString(pykey, "_sr_dict__container"));

    const auto& key = make_tuple("""
    
    counter=0 
    for ty in rec_vals:
        if ty == "long":
            cCode += "PyLong_AsLong(PyList_GetItem(pykey_data, " + str(counter) + ")), "
        elif ty == "double":
            cCode += "PyFloat_AsDouble(PyList_GetItem(pykey_data, " + str(counter) + ")), "
        elif len(ty) > 7 and ty[:7] == "VarChar":
            cCode += ty + "(PyUnicode_AsWideCharString(PyList_GetItem(pykey_data, " + str(counter) + "), NULL)), "
        else:
            print("Error: type is not supported!!: " + str(ty))
        counter += 1

    cCode = cCode[:-2] + ");"

    cCode += """
    (*(self->dict))[key] = true;

    Py_RETURN_TRUE;
}


static PyObject * """ + abbr + """_Get(""" + abbr + """ *self, PyObject *pykey)
{ 
    PyObject* pykey_data = PyDict_Values(PyObject_GetAttrString(pykey, "_sr_dict__container"));

    const auto& key = make_tuple("""
    
    counter=0 
    for ty in rec_vals:
        if ty == "long":
            cCode += "PyLong_AsLong(PyList_GetItem(pykey_data, " + str(counter) + ")), "
        elif ty == "double":
            cCode += "PyFloat_AsDouble(PyList_GetItem(pykey_data, " + str(counter) + ")), "
        elif len(ty) > 7 and ty[:7] == "VarChar":
            cCode += ty + "(PyUnicode_AsWideCharString(PyList_GetItem(pykey_data, " + str(counter) + "), NULL)), "
        else:
            print("Error: type is not supported!!: " + str(ty))
        counter += 1

    cCode = cCode[:-2] + ");"

    cCode += """
    if ((*(self->dict))[key] == true)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}



static PyObject * """ + abbr + """_FromDict(""" + abbr + """ *self, PyObject *data_dict)
{ 

    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while(auto pair = PyDict_Next(data_dict, &pos, &key, &value))
    {
        """ + abbr + """_Set(self, key, value);
    }


    Py_RETURN_TRUE;
}


static PyObject * """ + abbr + """_ToDict(""" + abbr + """ *self)
{ 
    PyObject* recType = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdql_lib")), "record");
    PyObject* srdictType = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdql_lib")), "sr_dict");

    PyObject* outdict = PyDict_New();

    for (auto& p : *(self->dict))
    {
        PyObject* tmprecdict = PyDict_New();
        """   
    counter=0 
    for ty in rec_vals:
        cCode += """PyDict_SetItemString(tmprecdict, ((self->cols)[""" + str(counter) + """]).c_str(), """
        if ty == "long":
            cCode += "PyLong_FromLong(get<" + str(counter) + ">(p.first)));\n\t\t"
        elif ty == "double":
            cCode += "PyFloat_FromDouble(get<" + str(counter) + ">(p.first)));\n\t\t"
        elif len(ty) > 7 and ty[:7] == "VarChar":
            number = re.search(r'VarChar<(.*)>', ty).group(1)
            cCode += "PyUnicode_FromWideChar((Py_UNICODE*)(get<" + str(counter) + ">(p.first)).data, " + number + "));\n\t\t"
        else:
            print("Error: type is not supported!: " + str(ty))
        counter += 1
    cCode += """

        PyDict_SetItem(outdict, PyObject_CallObject(recType, Py_BuildValue("(O)", tmprecdict)), Py_True);
    }

    return (PyObject*)PyObject_CallObject(srdictType, Py_BuildValue("(O)", outdict));
}


static PyObject * """ + abbr + """_Print(""" + abbr + """ *self)
{
    cout << *(self->dict) << endl;
    Py_RETURN_TRUE;
}

static PyObject * """ + abbr + """_Size(""" + abbr + """ *self)
{
    return PyLong_FromLong((*(self->dict)).size());
}

"""
    cCode += """

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

"""

    cCode += """

static PyObject* New_""" + abbr + """(PyObject *self, PyObject *args)
{
    return (PyObject*)(&""" + abbr + """_Type);
}

"""
    cCode += """
static PyObject* Set(PyObject *self, PyObject *args)
{
    PyObject * dict;
    PyObject * pykey;
    PyObject * pyval;

    if (!PyArg_ParseTuple(args, "OOO", &dict, &pykey, &pyval)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""

    cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Set(("""+ abbr + """*)dict, pykey, pyval);
        """

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""
 
    cCode += """
static PyObject* Get(PyObject *self, PyObject *args)
{
    PyObject * dict;
    PyObject * pykey;

    if (!PyArg_ParseTuple(args, "OO", &dict, &pykey)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""

    cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Get(("""+ abbr + """*)dict, pykey);
        """

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""

    cCode += """
static PyObject* FromDict(PyObject *self, PyObject *args)
{
    PyObject * dict;
    PyObject * data_dict;

    if (!PyArg_ParseTuple(args, "OO", &dict, &data_dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""
    
    cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_FromDict(("""+ abbr + """*)dict, data_dict);
        """

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""

 
    cCode += """
static PyObject* ToDict(PyObject *self, PyObject *args)
{
    PyObject * dict;

    if (!PyArg_ParseTuple(args, "O", &dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""
    cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_ToDict(("""+ abbr + """*)dict);
        """

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""

    cCode += """
static PyObject* Size(PyObject *self, PyObject *args)
{
    PyObject * dict;

    if (!PyArg_ParseTuple(args, "O", &dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Size");
        return Py_False;
    }
"""

    cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Size(("""+ abbr + """*)dict);
        """
    
    cCode += """
    else
    {
        cout << "Size: Type is not defined!" << endl;
        return Py_False;
    }
}

static PyObject* Print(PyObject *self, PyObject *args)
{
    PyObject * dict;

    if (!PyArg_ParseTuple(args, "O", &dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Size");
        return Py_False;
    }
"""

    cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Print(("""+ abbr + """*)dict);
        """
    cCode += """
    else
    {
        cout << "Print: Type is not defined!" << endl;
        return Py_False;
    }
}

static PyMethodDef fast_dict_methods[] = {
{"set", Set, METH_VARARGS, ""},
{"get", Get, METH_VARARGS, ""},
{"from_dict", FromDict, METH_VARARGS, ""},
{"to_dict", ToDict, METH_VARARGS, ""},
{"size", Size, METH_VARARGS, ""},
{"print", Print, METH_VARARGS, ""},"""
    cCode += """
{"new_""" + abbr + """\", New_""" + abbr + """, METH_VARARGS, ""},"""
    cCode += """{NULL,		NULL}		/* sentinel */
};

static struct PyModuleDef fast_dict = 
{
    PyModuleDef_HEAD_INIT,
    \"fast_dict\",
    "Documentation",
    -1,
    fast_dict_methods, NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit_""" + file_path[:-3] + """_fastdict_compiled(void) 
{
    """
    
    cCode += """
    """ + abbr + """_Type_init();  
    if (PyType_Ready(&""" + abbr + """_Type) < 0)
        cout << "Type Not Ready!" << endl;
    """
    cCode += """

    return PyModule_Create(&fast_dict);

}
"""

    dir_path = os.path.dirname(os.path.abspath(file_path))
    # print("Fast dict code:")
    # print(cCode)
    with open(dir_path + "/fast_dict.cpp", "w") as myfile:
        myfile.write(cCode)
        myfile.close()

    print(">>> fast_dict.py file generated")

################################################################################

if __name__ == "__main__":
    main()
