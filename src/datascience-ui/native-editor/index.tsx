// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
'use strict';

// This must be on top, do not change. Required by webpack.
import '../common/main';
// This must be on top, do not change. Required by webpack.

// tslint:disable-next-line: ordered-imports
import '../common/index.css';

import * as React from 'react';
import * as ReactDOM from 'react-dom';
import { Provider } from 'react-redux';

import { TextAreaFocusTracker } from '../interactive-common/textAreaFocusTracker';
import { WidgetManagerComponent } from '../ipywidgets/webViews/container';
import { IVsCodeApi, PostOffice } from '../react-common/postOffice';
import { detectBaseTheme } from '../react-common/themeDetector';
import { getConnectedNativeEditor } from './nativeEditor';
import { createStore } from './redux/store';

// This special function talks to vscode from a web panel
export declare function acquireVsCodeApi(): IVsCodeApi;
const baseTheme = detectBaseTheme();
// tslint:disable-next-line: no-any
const testMode = (window as any).inTestMode;
// tslint:disable-next-line: no-typeof-undefined
const skipDefault = testMode ? false : typeof acquireVsCodeApi !== 'undefined';

// Create the redux store
const postOffice = new PostOffice();
const store = createStore(skipDefault, baseTheme, testMode, postOffice);

// Wire up a connected react control for our NativeEditor
const ConnectedNativeEditor = getConnectedNativeEditor();

// Stick them all together
ReactDOM.render(
    <Provider store={store}>
        <TextAreaFocusTracker />
        <ConnectedNativeEditor />
        <WidgetManagerComponent postOffice={postOffice} widgetContainerId={'rootWidget'} store={store} />
    </Provider>,
    document.getElementById('root') as HTMLElement
);
