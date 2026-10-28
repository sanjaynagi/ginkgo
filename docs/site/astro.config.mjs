// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

// The site root is the forest home page, public/index.html. The docs keep the
// URLs they had under Sphinx: /docs/, /guide/..., /getting-started/...
export default defineConfig({
	site: 'https://sanjaynagi.github.io',
	base: '/ginkgo',
	trailingSlash: 'always',
	integrations: [
		starlight({
			title: 'Ginkgo',
			description: 'Scientific workflows written in plain Python, with graphs that grow as they run.',
			logo: { src: './src/assets/ginkgo-leaf.png' },
			favicon: '/_static/favicon.png',
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/sanjaynagi/ginkgo' }],
			customCss: ['./src/styles/ginkgo.css'],
			plugins: [starlightLinksValidator()],
			sidebar: [
				{ label: 'Getting started', items: ['docs', 'getting-started/installation', 'getting-started/quickstart', 'motivation'] },
				{
					label: 'Writing workflows',
					items: ['guide/concepts', 'guide/tasks-and-flows', 'guide/outputs', 'guide/environments', 'guide/resources'],
				},
				{
					label: 'Running and inspecting',
					items: ['guide/cli', 'guide/caching-and-provenance', 'guide/querying-provenance', 'guide/maintenance', 'guide/coding-agents'],
				},
				{ label: 'Examples', items: ['examples/bioinfo-workflow'] },
				{
					label: 'Plugins',
					items: ['guide/notebooks', 'guide/reports', 'guide/notifications', 'guide/remote-execution', 'guide/remote-oke'],
				},
				{ label: 'Reference', items: ['faq'] },
			],
		}),
	],
});
