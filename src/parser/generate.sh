#!/bin/bash

set -exuo pipefail


echo "Downloading DorisLexer.g4 and DorisParser.g4 ..."
curl -sSL https://raw.githubusercontent.com/apache/doris/master/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisLexer.g4 -o DorisLexer.g4.orig
curl -sSL https://raw.githubusercontent.com/apache/doris/master/fe/fe-core/src/main/antlr4/org/apache/doris/nereids/DorisParser.g4 -o DorisParser.g4.orig

echo "Modifying DorisLexer.g4 and DorisParser.g4 ..."
which sd || echo "please install https://github.com/chmln/sd first"

# Replace lexer @member
read -r -d '' lexerReplacer <<- EOF || true
options { caseInsensitive = true; }

@structmembers {
    /**
    * When true, parser should throw ParseExcetion for unclosed bracketed comment.
    */
    has_unclosed_bracketed_comment bool
}

@members {
var isNoBackslashEscapes = false

/**
* Verify whether current token is a valid decimal token (which contains dot).
* Returns true if the character that follows the token is not a digit or letter or underscore.
*
* For example:
* For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
* For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
* For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
* For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
* by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
* which is not a digit or letter or underscore.
*/
func (l *DorisLexer) isValidDecimal() bool {
    nextChar := l.GetInputStream().LA(1)
    if nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
        nextChar == '_' {
        return false
    } else {
        return true
    }
}

/**
* This method will be called when the character stream ends and try to find out the
* unclosed bracketed comment.
* If the method be called, it means the end of the entire character stream match,
* and we set the flag and fail later.
*/
func (l *DorisLexer) markUnclosedComment() {
    l.has_unclosed_bracketed_comment = true
}
}
EOF
cp DorisLexer.g4.orig DorisLexer.g4.new
sd -fms -n1 '^@members\s*\{.*^\}$' "$lexerReplacer" DorisLexer.g4.new
sd '\{isValidDecimal\(' '{p.isValidDecimal(' DorisLexer.g4.new
sd '\{markUnclosedComment\(' '{l.markUnclosedComment(' DorisLexer.g4.new
sd '^SIMPLE_COMMENT\n(.*)channel\(.*\)' "SIMPLE_COMMENT\n    : '--' ('\\\\\\\\\\\n' | ~[\\\r\\\n])* -> skip" DorisLexer.g4.new
sd '^(BRACKETED_COMMENT\n)(.*)channel\(.*\)' '$1${2}skip' DorisLexer.g4.new
mv DorisLexer.g4.new DorisLexer.g4

# Replace parser @member
read -r -d '' parserReplacer <<- EOF || true
@members {
var ansiSQLSyntax = false
}
EOF
cp DorisParser.g4.orig DorisParser.g4.new
sd -fms -n1 '^@members\s*\{.*^\}$' "$parserReplacer" DorisParser.g4.new
mv DorisParser.g4.new DorisParser.g4

echo "Generating Go antlr4 parser code ..."
antlr4 -Dlanguage=Go -package parser ./*.g4
