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

import React from 'react';
import AceEditor from 'react-ace';

import { parseStringToJSON, stringifyJSON, validJson } from '../../utils';

interface JSONInputProps extends React.Props<any> {
  onChange: (newJSONValue: any) => void;
  value: any;
  updateInputValidity?: (valueValid: boolean) => void;
  focus?: boolean;
  width?: string;
  height?: string;
}

interface JSONInputState {
  stringValue: string;
}

export class JSONInput extends React.PureComponent<JSONInputProps, JSONInputState> {
  constructor(props: JSONInputProps) {
    super(props);
    this.state = {
      stringValue: '',
    };
  }

  componentDidMount(): void {
    const { value } = this.props;
    const stringValue = stringifyJSON(value);
    this.setState({
      stringValue,
    });
  }

  componentWillReceiveProps(nextProps: JSONInputProps): void {
    if (JSON.stringify(nextProps.value) !== JSON.stringify(this.props.value)) {
      this.setState({
        stringValue: stringifyJSON(nextProps.value),
      });
    }
  }

  render() {
    const { onChange, updateInputValidity, focus, width, height } = this.props;
    const { stringValue } = this.state;
    return (
      <AceEditor
        key="hjson"
        mode="hjson"
        theme="solarized_dark"
        name="ace-editor"
        onChange={(e: string) => {
          this.setState({ stringValue: e });
          if (validJson(e) || e === '') onChange(parseStringToJSON(e));
          if (updateInputValidity) updateInputValidity(validJson(e) || e === '');
        }}
        focus={focus}
        fontSize={12}
        width={width || '100%'}
        height={height || '8vh'}
        showPrintMargin={false}
        showGutter={false}
        value={stringValue}
        editorProps={{
          $blockScrolling: Infinity,
        }}
        setOptions={{
          enableBasicAutocompletion: false,
          enableLiveAutocompletion: false,
          showLineNumbers: false,
          tabSize: 2,
        }}
        style={{}}
      />
    );
  }
}
