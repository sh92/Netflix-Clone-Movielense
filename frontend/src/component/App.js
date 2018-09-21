import React, { Component } from 'react';
import logo from '../logo.svg';
import '../css/App.css';
import GridComponent from './GridComponent';

class App extends Component {
  render() {
    return (
      <div className="App">
        <header className="App-header">
          <h1 className="App-title">MovieLens Clone tutorial</h1>
        </header>
        <GridComponent />
      </div>
    );
  }
}

export default App;
