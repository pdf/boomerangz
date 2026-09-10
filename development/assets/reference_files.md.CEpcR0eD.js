import{_ as n,o as s,c as e,a2 as p}from"./chunks/framework.DjaPWvpk.js";const g=JSON.parse('{"title":"Paths and files","description":"","frontmatter":{},"headers":[],"relativePath":"reference/files.md","filePath":"reference/files.md","lastUpdated":1788977791000}'),i={name:"reference/files.md"};function t(l,a,o,r,c,d){return s(),e("div",null,[...a[0]||(a[0]=[p(`<h1 id="paths-and-files" tabindex="-1">Paths and files <a class="header-anchor" href="#paths-and-files" aria-label="Permalink to &quot;Paths and files&quot;">​</a></h1><div class="vp-code-group vp-adaptive-theme"><div class="tabs"><input type="radio" name="group-B6kip" id="tab-w5GGrJq" checked><label data-title="Arch Linux / derivatives" for="tab-w5GGrJq">Arch Linux / derivatives</label></div><div class="blocks"><div class="language-text vp-adaptive-theme active"><button title="Copy Code" class="copy"></button><span class="lang">text</span><pre class="shiki shiki-themes github-light github-dark vp-code" tabindex="0"><code><span class="line"><span>/usr/bin/boomerangz</span></span>
<span class="line"><span>    Executable.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/usr/lib/boomerangz/boomerangz-shell</span></span>
<span class="line"><span>    Restricted login-shell wrapper for the packaged boomerangz account.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/etc/boomerangz/config.toml</span></span>
<span class="line"><span>    Protected primary configuration.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/etc/boomerangz/config.d/*.toml</span></span>
<span class="line"><span>    Configuration drop-ins, applied in filename order.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/etc/boomerangz/credentials.d/</span></span>
<span class="line"><span>    Imported pairing bundles and dedicated SSH credentials.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/var/lib/boomerangz/identity/</span></span>
<span class="line"><span>    Persistent installation identity and managed certificate material.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/run/boomerangz/boomerangz.sock</span></span>
<span class="line"><span>    Local status and control socket; boomerangz:boomerangz, mode 0660.</span></span>
<span class="line"><span></span></span>
<span class="line"><span>/usr/lib/systemd/system/boomerangz.service</span></span>
<span class="line"><span>    systemd service unit, including start and live-reload actions.</span></span></code></pre></div></div></div><p>The configuration file is owned by <code>root</code> and readable by the <code>boomerangz</code> group. Credential and identity material must not be world-accessible. Membership in the <code>boomerangz</code> group also grants full access to the local control API.</p><p>The package registers <code>/usr/lib/boomerangz/boomerangz-shell</code> in <code>/etc/shells</code> while installed. Do not assign the packaged account a general-purpose shell.</p><p>Preserve <code>/var/lib/boomerangz/identity</code> when restoring the same installation. Do not copy it to another simultaneously active host.</p>`,5)])])}const b=n(i,[["render",t]]);export{g as __pageData,b as default};
