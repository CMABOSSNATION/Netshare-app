/**
 * index.js — React Native entry point
 * REQUIRED: This file must exist in repo root for "react-native bundle" to work.
 * The workflow's bundle step uses --entry-file index.js
 */
import { AppRegistry } from 'react-native';
import App from './App';
import { name as appName } from './app.json';

AppRegistry.registerComponent(appName, () => App);
