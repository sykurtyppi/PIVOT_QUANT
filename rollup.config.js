import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import terser from '@rollup/plugin-terser';
import filesize from 'rollup-plugin-filesize';

const isProduction = process.env.NODE_ENV === 'production';

export default {
    input: 'src/index.js',
    output: [
        {
            file: 'dist/index.esm.js',
            format: 'esm',
            sourcemap: true
        },
        {
            file: 'dist/index.cjs',
            format: 'cjs',
            exports: 'named',
            sourcemap: true
        }
    ],
    plugins: [
        resolve(),
        commonjs(),
        isProduction && terser(),
        filesize()
    ].filter(Boolean)
};
