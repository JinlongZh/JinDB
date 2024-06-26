package com.jinlong.jindb.backend.parser;

import com.jinlong.jindb.backend.parser.statement.*;
import com.jinlong.jindb.common.ErrorConstants;

import java.util.ArrayList;
import java.util.List;

/**
 * 实现了对语句的解析
 *
 * @Author zjl
 * @Date 2024/6/24
 */
public class Parser {

    public static Object Parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch (token) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                default:
                    throw ErrorConstants.InvalidCommandException;
            }
        } catch (Exception e) {
            statErr = e;
        }
        try {
            String next = tokenizer.peek();
            if (!"".equals(next)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch (Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if (statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            return new Show();
        }
        throw ErrorConstants.InvalidCommandException;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if (!"set".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if (!"=".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if (!"from".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw ErrorConstants.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if (!"into".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw ErrorConstants.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if (!"values".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if ("".equals(value)) {
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if ("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while (true) {
                String field = tokenizer.peek();
                if (!isName(field)) {
                    throw ErrorConstants.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if (",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if (!"from".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw ErrorConstants.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if ("".equals(tmp)) {
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if (!"where".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if ("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if (!isLogicOp(logicOp)) {
            throw ErrorConstants.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if (!"".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();

        String field = tokenizer.peek();
        if (!isName(field)) {
            throw ErrorConstants.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if (!isCmpOp(op)) {
            throw ErrorConstants.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static Drop parseDrop(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if (!isName(tableName)) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }

        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static Create parseCreate(Tokenizer tokenizer) throws Exception {
        if (!"table".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        Create create = new Create();
        String name = tokenizer.peek();
        if (!isName(name)) {
            throw ErrorConstants.InvalidCommandException;
        }
        create.tableName = name;

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if ("(".equals(field)) {
                break;
            }

            if (!isName(field)) {
                throw ErrorConstants.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if (!isType(fieldType)) {
                throw ErrorConstants.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            String next = tokenizer.peek();
            if (",".equals(next)) {
                continue;
            } else if ("".equals(next)) {
                throw ErrorConstants.TableNoIndexException;
            } else if ("(".equals(next)) {
                break;
            } else {
                throw ErrorConstants.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);

        tokenizer.pop();
        if (!"index".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while (true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if (")".equals(field)) {
                break;
            }
            if (!isName(field)) {
                throw ErrorConstants.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if (!"".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        return create;
    }

    private static boolean isType(String tp) {
        return ("int32".equals(tp) || "int64".equals(tp) ||
                "string".equals(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if (!"".equals(tokenizer.peek())) {
            throw ErrorConstants.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        String isolation = tokenizer.peek();
        Begin begin = new Begin();
        if ("".equals(isolation)) {
            return begin;
        }
        if (!"isolation".equals(isolation)) {
            throw ErrorConstants.InvalidCommandException;
        }

        tokenizer.pop();
        String level = tokenizer.peek();
        if (!"level".equals(level)) {
            throw ErrorConstants.InvalidCommandException;
        }
        tokenizer.pop();

        String tmp1 = tokenizer.peek();
        if ("read".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("committed".equals(tmp2)) {
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw ErrorConstants.InvalidCommandException;
                }
                return begin;
            } else {
                throw ErrorConstants.InvalidCommandException;
            }
        } else if ("repeatable".equals(tmp1)) {
            tokenizer.pop();
            String tmp2 = tokenizer.peek();
            if ("read".equals(tmp2)) {
                begin.isRepeatableRead = true;
                tokenizer.pop();
                if (!"".equals(tokenizer.peek())) {
                    throw ErrorConstants.InvalidCommandException;
                }
                return begin;
            } else {
                throw ErrorConstants.InvalidCommandException;
            }
        } else {
            throw ErrorConstants.InvalidCommandException;
        }
    }

    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

}
