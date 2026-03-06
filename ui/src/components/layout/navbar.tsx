import { Link, NavLink } from "react-router";

const links = [
  { to: "/", label: "Traits" },
  { to: "/genes", label: "Genes" },
  { to: "/sources", label: "Sources" },
  { to: "/query", label: "Query" },
  { to: "/settings", label: "Settings" },
];

export function Navbar() {
  return (
    <div className="navbar bg-base-100 shadow-sm">
      <div className="container mx-auto flex items-center gap-2">
        <Link to="/" className="btn btn-ghost text-xl font-bold">
          PEGASUS V2F
        </Link>
        <ul className="menu menu-horizontal gap-1 px-1">
          {links.map((link) => (
            <li key={link.to}>
              <NavLink
                to={link.to}
                className={({ isActive }) =>
                  isActive ? "active font-semibold" : ""
                }
              >
                {link.label}
              </NavLink>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
