parser grammar MilvusParser;

options {
	tokenVocab = MilvusLexer;
}

ident: IDENTIFIER;

nullLiteral: NULL;

booleanLiteral: TRUE | FALSE;

numericLiteral: integerLiteral | DECIMAL_VALUE;

stringLiteral: SINGLE_QUOTED_STRING | DOUBLE_QUOTED_STRING;

integerLiteral:
	INTEGER_VALUE
	| OCTAL_VALUE
	| HEXADECIMAL_VALUE
	| BINARY_VALUE;
