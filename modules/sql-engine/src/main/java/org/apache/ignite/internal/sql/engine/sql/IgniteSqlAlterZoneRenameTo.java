/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.sql;

import java.util.List;
import java.util.Objects;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code ALTER ZONE RENAME TO} statement.
 */
public class IgniteSqlAlterZoneRenameTo extends IgniteAbstractSqlAlterZone {

    private final SqlIdentifier newName;

    /** Constructor. */
    public IgniteSqlAlterZoneRenameTo(SqlParserPos pos, SqlIdentifier name, SqlIdentifier newName, boolean ifExists) {
        super(pos, name, ifExists);
        this.newName = Objects.requireNonNull(newName, "newName");
    }

    /** {@inheritDoc} */
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, newName);
    }

    /**
     * The new name for a distribution zone.
     */
    public SqlIdentifier newName() {
        return newName;
    }

    /** {@inheritDoc} */
    @Override
    protected void unparseAlterZoneOperation(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("RENAME TO");

        newName.unparse(writer, leftPrec, rightPrec);
    }
}
