import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <div className="py-16 text-center">
      <div className="text-4xl font-bold text-accent mb-2">404</div>
      <div className="text-muted mb-6">This page doesn't exist.</div>
      <Link to="/" className="btn">
        Home
      </Link>
    </div>
  );
}
