lexer grammar MilvusLexer;

LIKE: 'LIKE';
EXISTS: 'EXISTS';
TEXTMATCH: 'TEXT_MATCH';
PHRASEMATCH: 'PHRASE_MATCH';
RANDOMSAMPLE: 'RANDOM_SAMPLE';
INTERVAL: 'INTERVAL';
ISO: 'ISO';
MINIMUM_SHOULD_MATCH: 'MINIMUM_SHOULD_MATCH';
IS: 'IS';
NULL: 'NULL';
NOT: '!' | 'NOT';
IN: 'IN';
TRUE: 'TRUE';
FALSE: 'FALSE';

DOLLAR: '$';
ASSIGN: '=';
EQ: '==';
NE: '!=';
LT: '<';
LE: '<=';
GT: '>';
GE: '>=';

ADD: '+';
SUB: '-';
MUL: '*';
DIV: '/';
MOD: '%';
POW: '**';
SHL: '<<';
SHR: '>>';
BAND: '&';
BOR: '|';
BXOR: '^';
BNOT: '~';

AND: '&&' | 'AND';
OR: '||' | 'OR';

OPEN_SQUARE_BRACKET: '[';
CLOSE_SQUARE_BRACKET: ']';
OPEN_PARENTHESIS: '(';
CLOSE_PARENTHESIS: ')';
OPEN_BRACE: '{';
CLOSE_BRACE: '}';

IDENTIFIER: (LETTER | '_') (LETTER | DIGIT | '_')*;

fragment DIGIT: [0-9];
fragment LETTER: [a-zA-Z];

WS : [ \t\r\n\f]+ -> channel(HIDDEN) ;
