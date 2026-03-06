import { Navbar } from "./components/layout/navbar";
import { AppRoutes } from "./routes";

export function App() {
  return (
    <div className="min-h-screen bg-base-200">
      <Navbar />
      <main className="container mx-auto px-4 py-6">
        <AppRoutes />
      </main>
    </div>
  );
}
