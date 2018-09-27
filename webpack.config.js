module.exports = {
    entry: [
        './frontend/jsx/index.jsx'
    ],
    module: {
        rules: [
            {
                test: /\.(js|jsx)$/,
                exclude: /node_modules/,
                use: ['babel-loader']
            }
        ]
    },
    output: {
        path: __dirname + '/frontend/static',
        filename: 'bundle.js'
    }
};
