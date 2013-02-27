#ifndef EXPR_H
#define EXPR_H

#include "dberror.h"
#include "tables.h"

// datatype for arguments of expressions used in conditions
typedef enum ExprType {
  EXPR_OP,
  EXPR_CONST,
  EXPR_ATTRREF
} ExprType;

typedef struct Expr {
  ExprType type;
  union expr {
    Value *cons;
    int attrRef;
    struct Operator *op;
  } expr;
} Expr;

// comparison operators
typedef enum OpType {
  OP_BOOL_AND,
  OP_BOOL_OR,
  OP_BOOL_NOT,
  OP_COMP_EQUAL,
  OP_COMP_SMALLER
} OpType;

typedef struct Operator {
  OpType type;
  Expr **args;
} Operator;



// expression evaluation methods
extern RC valueEquals (Value *left, Value *right, Value *result);
extern RC valueSmaller (Value *left, Value *right, Value *result);
extern RC boolNot (Value *input, Value *result);
extern RC boolAnd (Value *left, Value *right, Value *result);
extern RC boolOr (Value *left, Value *right, Value *result);
extern RC evalExpr (Record *record, Schema *schema, Expr *expr, Value *result);
extern RC freeExpr (Expr *expr);
extern void freeVal(Value *val);


#define CPVAL(result,input)						\
  do {									\
  result->dt = input->dt;						\
  switch(input->dt)                                                     \
    {									\
    case DT_INT:							\
      result->v.intV = input->v.intV;					\
      break;								\
    case DT_STRING:							\
      result->v.stringV = (char *) malloc(strlen(input->v.stringV));	\
      strcpy(result->v.stringV, input->v.stringV);			\
      break;								\
    case DT_FLOAT:							\
      result->v.floatV = input->v.floatV;				\
      break;								\
    case DT_BOOL:							\
      result->v.boolV = input->v.boolV;					\
      break;								\
    }									\
} while(0)

#define MAKE_BINOP_EXPR(result,left,right,optype)			\
    do {								\
      Operator *_op = (Operator *) malloc(sizeof(_op));			\
      result = (Expr *) malloc(sizeof(Expr));				\
      result->type = EXPR_OP;						\
      result->expr.op = _op;						\
      _op->type = optype;						\
      _op->args = (Expr **) malloc(2 * sizeof(Expr*));			\
      _op->args[0] = left;						\
      _op->args[1] = right;						\
    } while (0)

#define MAKE_UNOP_EXPR(result,input,optype)				\
  do {									\
    Operator *op = (Operator *) malloc(sizeof(op));			\
    result = (Expr *) malloc(sizeof(Expr));				\
    result->type = optype;						\
    result->expr.op = op;						\
    op->args = (Expr **) malloc(sizeof(Expr*));				\
    op->args[0] = input;						\
  } while (0)

#define MAKE_ATTRREF(result,attr)					\
  do {									\
  result = (Expr *) malloc(sizeof(Expr));				\
  result->type = EXPR_ATTRREF;						\
  result->expr.attrRef = attr;						\
  } while(0)

#define MAKE_CONS(result,value)						\
  do {									\
    result = (Expr *) malloc(sizeof(Expr));				\
    result->type = EXPR_CONST;						\
    result->expr.cons = value;						\
  } while(0)



#endif // EXPR
