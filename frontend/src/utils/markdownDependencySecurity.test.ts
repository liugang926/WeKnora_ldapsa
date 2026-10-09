import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import test from 'node:test'
import katex from 'katex'

test('KaTeX ignores inherited trust when rendering an untrusted link', () => {
  const previous = Object.getOwnPropertyDescriptor(Object.prototype, 'trust')
  Object.defineProperty(Object.prototype, 'trust', {
    value: true,
    writable: true,
    configurable: true,
  })
  try {
    const html = katex.renderToString(String.raw`\href{javascript:alert(1)}{click}`, {
      throwOnError: false,
    })
    // The source expression remains in MathML annotation text. Check the
    // executable HTML element rather than that deliberately preserved text.
    assert.doesNotMatch(html, /<a(?:\s|>)/i)
  } finally {
    if (previous) {
      Object.defineProperty(Object.prototype, 'trust', previous)
    } else {
      Reflect.deleteProperty(Object.prototype, 'trust')
    }
  }
})

test('Markdown and Mermaid consumers resolve the same patched KaTeX package', () => {
  const require = createRequire(import.meta.url)
  const directKatex = require.resolve('katex')
  for (const consumer of ['marked-katex-extension', 'mermaid']) {
    const consumerRequire = createRequire(require.resolve(consumer))
    assert.equal(consumerRequire.resolve('katex'), directKatex, consumer)
  }
})
