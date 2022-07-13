package ca.waterloo.dsg.graphflow.tuple;

import ca.waterloo.dsg.graphflow.parser.query.expressions.Expression;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.NodeVariable;
import ca.waterloo.dsg.graphflow.parser.query.expressions.NodeOrRelVariable.RelVariable;
import ca.waterloo.dsg.graphflow.util.datatype.DataType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Schema of a tuple or table, i.e., an unordered set of (variable variableName, {@link DataType)
 * pairs.
 */
public class Schema implements Serializable {

    private static final Logger logger = LogManager.getLogger(Schema.class);

    Map<String, Expression> varNameToExpressionMap;

    public Schema() {
        varNameToExpressionMap = new HashMap<>();
    }

    public boolean containsVarName(String varName) {
        return varNameToExpressionMap.containsKey(varName);
    }

    public boolean containsRelVarName(String varName) {
        return varNameToExpressionMap.containsKey(varName) &&
            (DataType.RELATIONSHIP == varNameToExpressionMap.get(varName).getDataType());
    }

    public boolean containsNodeVarName(String varName) {
        return varNameToExpressionMap.containsKey(varName) &&
            (DataType.NODE == varNameToExpressionMap.get(varName).getDataType());
    }

    public Expression getExpression(String varName) {
        if (!varNameToExpressionMap.containsKey(varName)) {
            throw new IllegalArgumentException("Trying to fetch Expression for varName: " +
                varName + " which is not present in the schema");
        }
        return varNameToExpressionMap.get(varName);
    }

    public List<String> getVarNames() {
        return new ArrayList<>(varNameToExpressionMap.keySet());
    }

    public Set<NodeVariable> getNodeVariables() {
        return varNameToExpressionMap.values().stream().filter(
            expression -> DataType.NODE == expression.getDataType()).
            map(expression -> (NodeVariable) expression)
            .collect(Collectors.toSet());
    }

    public NodeVariable getNodeVariable(String variable) {
        return (NodeVariable) varNameToExpressionMap.
            values().
            stream().
            filter(expression -> variable.equals(expression.getVariableName())).
            collect(Collectors.toList()).
            get(0);
    }

    public RelVariable getRelVariable(String variable, String otherVariable) {
        for (var variableName : varNameToExpressionMap.keySet()) {
            if (!(varNameToExpressionMap.get(variableName) instanceof RelVariable)) {
                continue;
            }
            var relVariable = (RelVariable) varNameToExpressionMap.get(variableName);
            var srcNodeName = relVariable.getSrcNode().getVariableName();
            var dstNodeName = relVariable.getDstNode().getVariableName();
            if ((variable.equals(srcNodeName) && otherVariable.equals(dstNodeName)) ||
                (variable.equals(dstNodeName) && otherVariable.equals(srcNodeName))) {
                return relVariable;
            }
        }
        return null;
    }

    public Set<RelVariable> getRelVariables(String variable) {
        return varNameToExpressionMap.
            values().
            stream().
            filter(expression -> DataType.RELATIONSHIP == expression.getDataType()).
            map(expression -> (RelVariable) expression).
            filter(expression -> expression.getSrcNode().getVariableName().equals(variable) ||
                expression.getDstNode().getVariableName().equals(variable)).
            collect(Collectors.toSet());
    }

    public Set<RelVariable> getRelVariables() {
        return varNameToExpressionMap.values().stream().filter(
            expression -> DataType.RELATIONSHIP == expression.getDataType()).
            map(expression -> (RelVariable) expression)
            .collect(Collectors.toSet());
    }

    public Set<Entry<String, Expression>> getVariablesInLexOrder() {
        return varNameToExpressionMap.entrySet();
    }

    public void addRelVariable(RelVariable relVariable) {
        add(relVariable.getVariableName(), relVariable);
        addNodeVariable(relVariable.getSrcNode());
        addNodeVariable(relVariable.getDstNode());
    }

    public void addNodeVariable(NodeVariable nodeVariable) {
        add(nodeVariable.getVariableName(), nodeVariable);
    }

    public void add(String varName, Expression expression) {
        if (null == varName || null == expression) {
            logger.error("inputs to " + this.getClass().getSimpleName() +
                ".addVariable(String varName, DataType resultDataType) contain a null. varName: "
                + varName + " expression: " + expression);
            return;
        }
        if (!containsVarName(varName)) {
            varNameToExpressionMap.put(varName, expression);
        } else {
            if (!expression.equals(varNameToExpressionMap.get(varName))) {
                throw new IllegalArgumentException("Trying to insert to variables to the schema " +
                    "with two different expressions. Variable variableName: " + varName + " " +
                    "Previous expression: " + varNameToExpressionMap.get(varName) + ". " +
                    "New type: " + expression);
            }
        }
    }

    public static Schema union(Schema schema1, Schema schema2) {
        Schema unionedSchema = new Schema();
        schema1.varNameToExpressionMap.forEach((key, value) ->
            unionedSchema.varNameToExpressionMap.put(key, value));
        schema2.varNameToExpressionMap.forEach((key, value) -> {
            if (unionedSchema.varNameToExpressionMap.containsKey(key) &&
                !unionedSchema.varNameToExpressionMap.get(key).equals(value)) {
                throw new IllegalArgumentException("Trying to union two schemas with variables " +
                    "that have inconsistent types. Schema1's data type for param: " +
                    key + " is " + unionedSchema.varNameToExpressionMap.get(key) +
                    "; the type in schema2 is: " + value);
            }
            unionedSchema.varNameToExpressionMap.put(key, value);
        });
        return unionedSchema;
    }

    public boolean isEmpty() {
        return varNameToExpressionMap.isEmpty();
    }

    public Schema copy() {
        Schema copy = new Schema();
        this.varNameToExpressionMap.forEach((key, value)
            -> copy.varNameToExpressionMap.put(key, value));
        return copy;
    }

    /**
     * isSame() just checks the varNames and corresponding dataType of the expressions in the
     * varNameToExpressionMap.
     */
    public boolean isSame(Schema other) {
        if (this.varNameToExpressionMap.size() != other.varNameToExpressionMap.size()) {
            return false;
        }
        if (!this.varNameToExpressionMap.keySet().equals(other.varNameToExpressionMap.keySet())) {
            return false;
        }
        for (var key : varNameToExpressionMap.keySet()) {
            if (varNameToExpressionMap.get(key).getDataType() !=
                other.varNameToExpressionMap.get(key).getDataType()) {
                return false;
            }
        }
        return true;
    }
}
