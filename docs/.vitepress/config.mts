import { defineConfig } from 'vitepress'

const docsVersion = process.env.DOCS_VERSION || 'Development'
const docsBase = process.env.DOCS_BASE || '/'
const canonicalBase = process.env.DOCS_CANONICAL_BASE || docsBase
const siteOrigin = 'https://boomerangz.org'
// Only builds that track current docs (the root site and /development/) link
// to the editor; an archived version's page may no longer match main.
const docsEditLink = process.env.DOCS_EDIT_LINK === '1'

export default defineConfig({
  base: docsBase,
  cleanUrls: true,
  lang: 'en-US',
  title: 'boomerangz',
  description: 'A ZFS snapshot/replication manager to help make sure your data comes back to you.',
  lastUpdated: true,
  head: [
    ['link', { rel: 'icon', href: `${docsBase}favicon.svg`, type: 'image/svg+xml' }],
    ['meta', { name: 'theme-color', content: '#d69e00' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:title', content: 'boomerangz' }],
    ['meta', {
      property: 'og:description',
      content: 'A ZFS snapshot/replication manager to help make sure your data comes back to you.'
    }]
  ],
  transformHead({ pageData }) {
    const pagePath = pageData.relativePath
      .replace(/(^|\/)index\.md$/, '$1')
      .replace(/\.md$/, '')
    const canonical = `${siteOrigin}${canonicalBase}${pagePath}`
    return [
      ['link', { rel: 'canonical', href: canonical }],
      ['meta', { property: 'og:url', content: canonical }],
      ['meta', {
        property: 'og:image',
        content: `${siteOrigin}${docsBase}brand/boomerangz-social.png`
      }]
    ]
  },
  themeConfig: {
    logo: {
      light: '/brand/boomerangz-mark.svg',
      dark: '/brand/boomerangz-mark.svg',
      alt: 'Boomerangz'
    },
    nav: [
      { text: 'Guide', link: '/getting-started/' },
      { text: 'Reference', link: '/reference/cli' },
      { text: 'About', link: '/about' },
      { component: 'VersionSelector', props: { current: docsVersion } }
    ],
    sidebar: [
      {
        text: 'Start here',
        items: [
          { text: 'Getting started', link: '/getting-started/' },
          { text: 'Install', link: '/getting-started/installation' }
        ]
      },
      {
        text: 'Setup',
        items: [
          { text: 'Configure the daemon', link: '/guide/configuration' },
          { text: 'Manage datasets', link: '/guide/datasets' },
          { text: 'Configure destinations', link: '/guide/remotes' },
          { text: 'Secure your deployment', link: '/guide/security' }
        ]
      },
      {
        text: 'Operate',
        items: [
          { text: 'General', link: '/operations/' },
          { text: 'Recovery and maintenance', link: '/operations/recovery' },
          { text: 'Upgrade notes', link: '/operations/upgrading' },
          { text: 'Troubleshooting', link: '/operations/troubleshooting' }
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'Command line', link: '/reference/cli' },
          { text: 'Configuration', link: '/reference/configuration' },
          { text: 'ZFS properties', link: '/reference/properties' },
          { text: 'Paths and files', link: '/reference/files' },
          { text: 'Glossary', link: '/reference/glossary' }
        ]
      }
    ],
    search: { provider: 'local' },
    // GitHub's web editor only accepts branches, and a release build's ref is a
    // tag, so edits are always proposed against main.
    editLink: docsEditLink
      ? {
          pattern: 'https://github.com/pdf/boomerangz/edit/main/docs/:path',
          text: 'Edit this page on GitHub'
        }
      : undefined,
    socialLinks: [
      { icon: 'github', link: 'https://github.com/pdf/boomerangz' }
    ],
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © 2026 boomerangz contributors'
    },
    outline: { level: [2, 3], label: 'On this page' }
  }
})
