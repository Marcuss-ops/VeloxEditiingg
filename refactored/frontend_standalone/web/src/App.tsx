export default function App() {
  return (
    <main className="velox-boot">
      <h1>Velox frontend boot</h1>
      <p>
        This is the embedded SPA skeleton inside{' '}
        <code>refactored/frontend_standalone/web/</code>. The vite build emitted
        a working bundle; VELOX_SPA_DIR is happy.
      </p>
      <p>
        Replace this toy page with the real Creator&nbsp;Studio / YouTube&nbsp;Manager /
        Drive / Livestream UI. All Radix primitives,{' '}
        <code>react-router-dom</code>, <code>@tanstack/react-query</code>,{' '}
        <code>chart.js</code>, <code>motion</code>, and Tailwind utilities are
        already declared in <code>web/package.json</code> and resolved by the
        synced <code>web/package-lock.json</code>.
      </p>
    </main>
  )
}
