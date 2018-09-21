import React, { Component } from 'react';
import { Grid, Row, Col } from 'react-flexbox-grid';

class GridComponent extends Component {
  render() {
    return (
      <Grid fluid>
        <Row around="xs">
          <Col xs={2}>
            Box1
          </Col>
          <Col xs={2}>
            Box2
          </Col>
          <Col xs={2}>
            Box3
          </Col>
          <Col xs={2}>
            Box4
          </Col>
          <Col xs={2}>
            Box5
          </Col>
        </Row>
        <Row around="xs">
          <Col xs={2}>
            Box1
          </Col>
          <Col xs={2}>
            Box2
          </Col>
          <Col xs={2}>
            Box3
          </Col>
          <Col xs={2}>
            Box4
          </Col>
          <Col xs={2}>
            Box5
          </Col>
        </Row>
      </Grid>
      
      
    );
  }
}
export default GridComponent
