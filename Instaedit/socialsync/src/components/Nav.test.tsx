import { describe, expect, it, vi, beforeAll } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Nav } from "./Nav";

function renderNav() {
  return render(<Nav />);
}

describe("Nav", () => {
  beforeAll(() => {
    // jsdom doesn't implement scrollIntoView.
    Object.defineProperty(window.HTMLElement.prototype, "scrollIntoView", {
      value: vi.fn(),
      writable: true,
      configurable: true,
    });
  });

  it("renders the SocialSync logo", () => {
    renderNav();
    const logo = screen.getByText("SocialSync");
    expect(logo.closest("a")?.getAttribute("href")).toBe("#");
  });

  it("renders the desktop nav links", () => {
    renderNav();
    for (const label of ["Funzioni", "Come funziona", "Prezzi", "Accedi"]) {
      const matches = screen.getAllByText(label);
      expect(matches.length).toBeGreaterThan(0);
    }
  });

  it("renders the Inizia gratis CTA", () => {
    renderNav();
    const ctas = screen.getAllByText("Inizia gratis");
    expect(ctas.length).toBeGreaterThanOrEqual(1);
  });

  it("shows the mobile menu toggle button", () => {
    renderNav();
    expect(screen.getByTestId("mobile-menu-toggle")).toBeDefined();
  });

  it("opens the mobile menu when the toggle is clicked", async () => {
    renderNav();
    const user = userEvent.setup();

    const mobileMenu = screen.getByTestId("mobile-menu");
    expect(mobileMenu.classList.contains("hidden")).toBe(true);

    await user.click(screen.getByTestId("mobile-menu-toggle"));

    expect(mobileMenu.classList.contains("flex")).toBe(true);
    expect(mobileMenu.classList.contains("hidden")).toBe(false);
  });

  it("closes the mobile menu when a link is clicked", async () => {
    renderNav();
    const user = userEvent.setup();

    await user.click(screen.getByTestId("mobile-menu-toggle"));
    expect(screen.getByTestId("mobile-menu").classList.contains("flex")).toBe(true);

    // Click the first mobile link (Funzioni).
    const funzioniLinks = screen.getAllByText("Funzioni");
    await user.click(funzioniLinks[funzioniLinks.length - 1]);

    expect(screen.getByTestId("mobile-menu").classList.contains("hidden")).toBe(true);
  });

  it("closes the mobile menu when toggle is clicked again", async () => {
    renderNav();
    const user = userEvent.setup();

    await user.click(screen.getByTestId("mobile-menu-toggle")); // open
    expect(screen.getByTestId("mobile-menu").classList.contains("flex")).toBe(true);

    await user.click(screen.getByTestId("mobile-menu-toggle")); // close
    expect(screen.getByTestId("mobile-menu").classList.contains("hidden")).toBe(true);
  });
});
