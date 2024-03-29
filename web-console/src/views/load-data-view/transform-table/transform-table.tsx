/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import classNames from 'classnames';
import React from 'react';
import ReactTable from 'react-table';

import { TableCell } from '../../../components';
import { caseInsensitiveContains, filterMap } from '../../../utils';
import { escapeColumnName } from '../../../utils/druid-expression';
import { Transform } from '../../../utils/ingestion-spec';
import { HeaderAndRows } from '../../../utils/sampler';

import './transform-table.scss';

export interface TransformTableProps extends React.Props<any> {
  sampleData: HeaderAndRows;
  columnFilter: string;
  transformedColumnsOnly: boolean;
  transforms: Transform[];
  selectedTransformIndex: number;
  onTransformSelect: (transform: Transform, index: number) => void;
}

export class TransformTable extends React.PureComponent<TransformTableProps> {
  render() {
    const {
      sampleData,
      columnFilter,
      transformedColumnsOnly,
      transforms,
      selectedTransformIndex,
      onTransformSelect,
    } = this.props;

    return (
      <ReactTable
        className="transform-table -striped -highlight"
        data={sampleData.rows}
        columns={filterMap(sampleData.header, (columnName, i) => {
          if (!caseInsensitiveContains(columnName, columnFilter)) return null;
          const timestamp = columnName === '__time';
          const transformIndex = transforms.findIndex(f => f.name === columnName);
          if (transformIndex === -1 && transformedColumnsOnly) return null;
          const transform = transforms[transformIndex];

          const columnClassName = classNames({
            transformed: transform,
            selected: transform && transformIndex === selectedTransformIndex,
          });
          return {
            Header: (
              <div
                className={classNames('clickable')}
                onClick={() => {
                  if (transform) {
                    onTransformSelect(transform, transformIndex);
                  } else {
                    onTransformSelect(
                      {
                        type: 'expression',
                        name: columnName,
                        expression: escapeColumnName(columnName),
                      },
                      transformIndex,
                    );
                  }
                }}
              >
                <div className="column-name">{columnName}</div>
                <div className="column-detail">
                  {transform ? `= ${transform.expression}` : ''}&nbsp;
                </div>
              </div>
            ),
            headerClassName: columnClassName,
            className: columnClassName,
            id: String(i),
            accessor: row => (row.parsed ? row.parsed[columnName] : null),
            Cell: row => <TableCell value={row.value} timestamp={timestamp} />,
          };
        })}
        defaultPageSize={50}
        showPagination={false}
        sortable={false}
      />
    );
  }
}
